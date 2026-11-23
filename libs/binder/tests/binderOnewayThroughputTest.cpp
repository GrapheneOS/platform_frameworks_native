/*
 * Measures how fast a process drains oneway transactions that are queued for a single binder
 * object.
 *
 * The kernel delivers oneway transactions for one binder object one at a time. While one is being
 * handled, the rest wait in the oneway half of the receiver's binder buffer, and the next one is
 * released when the receiver frees the buffer of the previous one. If a sender outpaces the
 * receiver, that space fills up and oneway transactions to the receiving process fail for every
 * sender.
 *
 * A server process registers one binder object with a pool of looper threads. The client keeps a
 * queue of oneway transactions outstanding for that object. The test reports the rate at which the
 * server receives them, how long they waited, and how often consecutive transactions were received
 * by different threads, together with the CPU time and the number of times its threads went to
 * sleep, per transaction.
 *
 * By default the handler does no work, so the result is the cost of delivery. The -w option gives
 * it a fixed amount of computation, which takes longer on a slower core, as a real handler does.
 * The amount is measured at the start of each run, so a hot or loaded core gives a run less work.
 * To compare runs, measure it once with -k and pass the result to every run with -i.
 *
 * The client sleeps whenever it has to wait for the server, so that the CPU time and energy of a
 * run come from sending, delivering and handling transactions.
 *
 * For stress runs, -o spreads the transactions over several binder objects, which are then
 * handled at the same time, and -p makes the server move its own memory to swap again and again.
 * Every run checks that each object received its transactions in order and one at a time.
 *
 * As a control, -y 1 sends synchronous transactions instead, with the same handler, so that one is
 * outstanding at a time and the client waits for each reply.
 *
 * Needs to run as root, since it registers a service with servicemanager.
 */

#include <binder/Binder.h>
#include <binder/IBinder.h>
#include <binder/IPCThreadState.h>
#include <binder/IServiceManager.h>
#include <binder/Parcel.h>
#include <binder/ProcessState.h>
#include <utils/String16.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <new>
#include <string>
#include <thread>
#include <vector>

#include <sched.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

using namespace android;

namespace {

enum {
    ONEWAY_PING = IBinder::FIRST_CALL_TRANSACTION,
};

constexpr int64_t kNanosPerSecond = 1000000000;
constexpr int64_t kNanosPerMicro = 1000;
constexpr double kMillisPerSecond = 1000;
constexpr int64_t kStallTimeoutNs = 10 * kNanosPerSecond;
// How long the client sleeps while the server works through the outstanding transactions. Short
// enough that the server does not run out of them in the meantime.
constexpr useconds_t kCatchUpSleepUs = 100;
constexpr useconds_t kPollSleepUs = 1000;
// How often the server looks whether the run is over. Rare enough that these wakeups do not show
// in its count of sleeps.
constexpr useconds_t kServerPollSleepUs = 20000;
constexpr size_t kPercent = 100;
constexpr size_t kMaxOptionValue = UINT32_MAX;
constexpr size_t kMaxThreads = 1024;
constexpr size_t kMaxObjects = 1024;
// Every transaction starts with the time it was sent and its number among those for its object.
constexpr size_t kHeaderSize = 2 * sizeof(int64_t);

struct Options {
    size_t calls = 100000;
    size_t threads = 16;
    size_t payloadSize = 140;
    // Maximum number of transactions sent but not yet received. 0 means no limit.
    size_t maxOutstanding = 1000;
    // Transactions per second. 0 means as fast as maxOutstanding allows.
    size_t rate = 0;
    // Computation in the handler for each transaction, as the time it takes in microseconds on the
    // core that measured it when the test started.
    size_t workUs = 0;
    // The same computation as a fixed number of iterations, so that runs on different drivers,
    // boots or loads do the same work. Replaces workUs when not 0.
    size_t iterations = 0;
    // Print how many iterations take this many microseconds on the server cores, and exit.
    size_t calibrateUs = 0;
    // Bit mask of the cores that the server runs on, with the client on the others. 0 leaves both
    // where the scheduler puts them.
    size_t serverCores = 0;
    // Number of binder objects in the server. The client sends to them in turn.
    size_t objects = 1;
    // How often the server asks the kernel to move its own memory to swap, in milliseconds. 0
    // means never.
    size_t pageOutMs = 0;
    // 1 sends synchronous transactions instead of oneway ones, and waits for each reply.
    size_t sync = 0;
};

// CPU time, sleeps and major page faults of a process, all of its threads together.
struct Usage {
    int64_t cpuNs;
    int64_t sleeps;
    int64_t majorFaults;
};

struct Record {
    int32_t object;
    int32_t tid;
    int64_t sendNs;
    int64_t receiveNs;
};

// Lives in memory shared between the client and the server process.
struct Shared {
    std::atomic<bool> serverReady;
    // Number of transactions whose handler has started, of those whose record is written, and of
    // those whose handler has finished its work.
    std::atomic<uint64_t> started;
    std::atomic<uint64_t> received;
    std::atomic<uint64_t> handled;
    // Transactions that an object received with another number than the next one, and
    // transactions that started while another one for the same object was still being handled.
    std::atomic<uint64_t> outOfOrder;
    std::atomic<uint64_t> overlapped;
    // Set by the client when the run is over. The server then publishes serverUsage.
    std::atomic<bool> finished;
    std::atomic<bool> serverUsageReady;
    Usage serverUsage;
    size_t capacity;
    Record records[];
};

int64_t nowNs() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * kNanosPerSecond + ts.tv_nsec;
}

int64_t toNs(const struct timeval& time) {
    return time.tv_sec * kNanosPerSecond + time.tv_usec * kNanosPerMicro;
}

// A thread goes to sleep with a voluntary context switch, for example when it waits for work in
// the binder driver.
Usage usageOfThisProcess() {
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    return {toNs(usage.ru_utime) + toNs(usage.ru_stime), usage.ru_nvcsw, usage.ru_majflt};
}

Usage operator-(const Usage& after, const Usage& before) {
    return {after.cpuNs - before.cpuNs, after.sleeps - before.sleeps,
            after.majorFaults - before.majorFaults};
}

// A fixed amount of computation, one multiplication and addition per iteration.
//
// Not inlined, and the value is hidden from the optimizer after every iteration. Otherwise the
// compiler folds several iterations into one, by a different factor at each call site, and the
// iterations that iterationsPerMicro() times are not the ones that the handler runs.
__attribute__((noinline)) uint64_t compute(uint64_t iterations) {
    // Multiplier and increment of a 64 bit linear congruential generator.
    constexpr uint64_t kMultiplier = 6364136223846793005;
    constexpr uint64_t kIncrement = 1442695040888963407;
    uint64_t value = iterations;
    for (uint64_t i = 0; i < iterations; i++) {
        value = value * kMultiplier + kIncrement;
        __asm__ volatile("" : "+r"(value));
    }
    return value;
}

// Returns how many iterations of compute() take one microsecond on the core that this runs on at
// its full speed. The probes together run long enough for the core to reach that speed, and the
// fastest one counts.
uint64_t iterationsPerMicro() {
    constexpr uint64_t kProbeIterations = 1000000;
    constexpr int kProbes = 100;
    int64_t fastestNs = INT64_MAX;
    for (int i = 0; i < kProbes; i++) {
        const int64_t startNs = nowNs();
        compute(kProbeIterations);
        fastestNs = std::min(fastestNs, nowNs() - startNs);
    }
    return std::max<uint64_t>(1, kProbeIterations * kNanosPerMicro / fastestNs);
}

uint64_t handlerIterations(const Options& options) {
    return options.iterations > 0 ? options.iterations : options.workUs * iterationsPerMicro();
}

class OnewaySink : public BBinder {
public:
    OnewaySink(Shared* shared, uint64_t workIterations, int32_t object)
          : mShared(shared), mWorkIterations(workIterations), mObject(object) {}

    status_t onTransact(uint32_t code, const Parcel& data, Parcel* reply, uint32_t flags) override {
        if (code != ONEWAY_PING) {
            return BBinder::onTransact(code, data, reply, flags);
        }
        const int64_t receiveNs = nowNs();
        const int64_t sendNs = data.readInt64();
        const int64_t number = data.readInt64();

        // The driver delivers the oneway transactions for one binder object in the order they
        // were sent and one at a time. mBusy detects a break of that rule, and mNext is atomic so
        // that two handlers running at once are reported instead of racing on it.
        if (mBusy.exchange(true, std::memory_order_acq_rel)) {
            mShared->overlapped.fetch_add(1, std::memory_order_relaxed);
        }
        if (number != mNext.load(std::memory_order_relaxed)) {
            mShared->outOfOrder.fetch_add(1, std::memory_order_relaxed);
        }
        mNext.store(number + 1, std::memory_order_relaxed);

        // Handlers of different objects run at the same time, so each reserves its record.
        const uint64_t index = mShared->started.fetch_add(1, std::memory_order_relaxed);
        if (index < mShared->capacity) {
            mShared->records[index] = {mObject, gettid(), sendNs, receiveNs};
        }
        mShared->received.fetch_add(1, std::memory_order_release);

        // The buffer of this transaction is only freed after the handler returns, so the next one
        // for this object is not released before the work is done.
        compute(mWorkIterations);
        mShared->handled.fetch_add(1, std::memory_order_release);
        mBusy.store(false, std::memory_order_release);
        return OK;
    }

private:
    Shared* const mShared;
    const uint64_t mWorkIterations;
    const int32_t mObject;
    std::atomic<int64_t> mNext = 0;
    std::atomic<bool> mBusy = false;
};

String16 objectName(const std::string& base, size_t object) {
    return String16((base + "." + std::to_string(object)).c_str());
}

// Asks the kernel to move the private memory of this process to swap. That includes the heap,
// where the looper threads have their read buffers, and the stacks. The next access to such a
// page takes a major page fault, also when it is the binder driver that writes to a read buffer.
// This puts the server under memory pressure without taking memory from the rest of the system.
void pageOutOwnMemory() {
    FILE* maps = fopen("/proc/self/maps", "re");
    if (maps == nullptr) {
        return;
    }
    char line[512];
    while (fgets(line, sizeof(line), maps) != nullptr) {
        uintptr_t start = 0;
        uintptr_t end = 0;
        char permissions[5] = {};
        char name[256] = {};
        // The fields are the address range, the permissions, three that are not needed, and the
        // name, which is missing for mappings without one.
        if (sscanf(line, "%" SCNxPTR "-%" SCNxPTR " %4s %*s %*s %*s %255[^\n]", &start, &end,
                   permissions, name) < 3) {
            continue;
        }
        const bool anonymous =
                name[0] == '\0' || strncmp(name, "[anon", 5) == 0 || strcmp(name, "[heap]") == 0;
        if (anonymous && strcmp(permissions, "rw-p") == 0) {
            madvise(reinterpret_cast<void*>(start), end - start, MADV_PAGEOUT);
        }
    }
    fclose(maps);
}

// Returns whether the thread is blocked. For a looper thread that has joined the pool, this means
// that it is waiting for work in the binder driver.
bool isSleeping(pid_t tid) {
    const std::string path = "/proc/self/task/" + std::to_string(tid) + "/stat";
    FILE* file = fopen(path.c_str(), "re");
    if (file == nullptr) {
        return false;
    }
    char state = 0;
    // The second field is the thread name in parentheses.
    const int matched = fscanf(file, "%*d (%*[^)]) %c", &state);
    fclose(file);
    return matched == 1 && state == 'S';
}

[[noreturn]] void runServer(const Options& options, uint64_t workIterations, Shared* shared,
                            const std::string& name) {
    // Do not outlive the client.
    prctl(PR_SET_PDEATHSIG, SIGKILL);

    // Only the threads created here handle transactions, so that the pool has a fixed size and is
    // idle when the test starts.
    ProcessState::self()->setThreadPoolMaxThreadCount(0);

    for (size_t object = 0; object < options.objects; object++) {
        const sp<IBinder> sink =
                sp<OnewaySink>::make(shared, workIterations, static_cast<int32_t>(object));
        if (defaultServiceManager()->addService(objectName(name, object), sink) != OK) {
            fprintf(stderr, "Unable to register the service. Is the test running as root?\n");
            exit(EXIT_FAILURE);
        }
    }

    std::vector<std::atomic<pid_t>> loopers(options.threads);
    for (std::atomic<pid_t>& looper : loopers) {
        std::thread([&looper] {
            looper.store(gettid(), std::memory_order_release);
            IPCThreadState::self()->joinThreadPool(true);
        }).detach();
    }

    // The test measures delivery to a pool of idle threads. Do not let the client start until
    // every looper is waiting in the driver.
    const auto idle = [](const std::atomic<pid_t>& looper) {
        const pid_t tid = looper.load(std::memory_order_acquire);
        return tid != 0 && isSleeping(tid);
    };
    const int64_t deadlineNs = nowNs() + kStallTimeoutNs;
    while (!std::all_of(loopers.begin(), loopers.end(), idle)) {
        if (nowNs() > deadlineNs) {
            fprintf(stderr, "The looper threads did not become idle.\n");
            exit(EXIT_FAILURE);
        }
        usleep(kPollSleepUs);
    }
    if (options.pageOutMs > 0) {
        std::thread([&options] {
            for (;;) {
                usleep(static_cast<useconds_t>(options.pageOutMs * kMillisPerSecond));
                pageOutOwnMemory();
            }
        }).detach();
    }
    const Usage before = usageOfThisProcess();
    shared->serverReady.store(true, std::memory_order_release);

    while (!shared->finished.load(std::memory_order_acquire)) {
        usleep(kServerPollSleepUs);
    }
    shared->serverUsage = usageOfThisProcess() - before;
    shared->serverUsageReady.store(true, std::memory_order_release);
    for (;;) {
        pause();
    }
}

struct SendResult {
    size_t sent = 0;
    size_t rejected = 0;
    int64_t startNs = 0;
    int64_t endNs = 0;
    bool stalled = false;
};

void sleepUntil(int64_t dueNs) {
    const struct timespec due = {.tv_sec = static_cast<time_t>(dueNs / kNanosPerSecond),
                                 .tv_nsec = static_cast<long>(dueNs % kNanosPerSecond)};
    clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &due, nullptr);
}

// Sleeps until `count`, one of the counters of the server, reaches at least `target`. Returns false
// if the server stops counting before that.
bool waitForCount(const std::atomic<uint64_t>& count, uint64_t target, useconds_t sleepUs) {
    uint64_t current = count.load(std::memory_order_acquire);
    int64_t progressNs = nowNs();
    while (current < target) {
        usleep(sleepUs);
        const uint64_t before = current;
        current = count.load(std::memory_order_acquire);
        if (current != before) {
            progressNs = nowNs();
        } else if (nowNs() - progressNs > kStallTimeoutNs) {
            return false;
        }
    }
    return true;
}

// Returns whether another transaction may be sent, after waiting for the server if the limit on
// outstanding transactions is reached. Returns false if the server stops receiving.
bool waitForRoom(const Options& options, const Shared* shared, size_t sent) {
    if (options.maxOutstanding == 0 || sent < options.maxOutstanding) {
        return true;
    }
    return waitForCount(shared->received, sent - options.maxOutstanding + 1, kCatchUpSleepUs);
}

SendResult runClient(const Options& options, const Shared* shared,
                     const std::vector<sp<IBinder>>& sinks) {
    const std::vector<uint8_t> padding(
            options.payloadSize > kHeaderSize ? options.payloadSize - kHeaderSize : 0);
    // The number of the next transaction for each object. A rejected one does not use a number.
    std::vector<int64_t> next(sinks.size(), 0);
    const int64_t intervalNs =
            options.rate > 0 ? kNanosPerSecond / static_cast<int64_t>(options.rate) : 0;
    const uint32_t flags = options.sync ? 0 : IBinder::FLAG_ONEWAY;

    SendResult result;
    result.startNs = nowNs();
    for (size_t i = 0; i < options.calls; i++) {
        if (intervalNs > 0) {
            sleepUntil(result.startNs + static_cast<int64_t>(i) * intervalNs);
        }
        if (!waitForRoom(options, shared, result.sent)) {
            result.stalled = true;
            break;
        }

        const size_t object = i % sinks.size();
        Parcel data;
        data.writeInt64(nowNs());
        data.writeInt64(next[object]);
        if (!padding.empty()) {
            data.write(padding.data(), padding.size());
        }
        Parcel reply;
        if (sinks[object]->transact(ONEWAY_PING, data, &reply, flags) == OK) {
            result.sent++;
            next[object]++;
        } else {
            result.rejected++;
        }
    }
    result.endNs = nowNs();
    return result;
}

void printSettings(const Options& options, uint64_t workIterations) {
    printf("threads: %zu, objects: %zu, payload: %zu bytes, calls: %zu, %s, max outstanding: %zu, ",
           options.threads, options.objects, options.payloadSize, options.calls,
           options.sync ? "synchronous" : "oneway", options.maxOutstanding);
    if (options.iterations > 0) {
        printf("handler work: %" PRIu64 " iterations, ", workIterations);
    } else {
        printf("handler work: %zu us (%" PRIu64 " iterations), ", options.workUs, workIterations);
    }
    printf("server cores: %zx, page out: every %zu ms, rate: ", options.serverCores,
           options.pageOutMs);
    if (options.rate > 0) {
        printf("%zu/s\n", options.rate);
    } else {
        printf("unlimited\n");
    }
}

// Ends a line with the time that `count` transactions took and the resulting rate.
void printElapsed(size_t count, int64_t durationNs) {
    const double seconds = static_cast<double>(durationNs) / kNanosPerSecond;
    printf("in %.1f ms (%.0f/s)\n", seconds * kMillisPerSecond, seconds > 0 ? count / seconds : 0);
}

// Prints how the received transactions were spread over threads and how long they waited. A
// transaction is compared with the previous one for the same binder object.
void printDelivery(const Record* records, size_t received, size_t objects) {
    constexpr size_t kHighPercentile = 99;

    size_t threadChanges = 0;
    size_t compared = 0;
    // 0 is not a thread id, and stands for an object that has received nothing yet.
    std::vector<int32_t> previousTid(objects, 0);
    std::vector<int32_t> tids;
    std::vector<int64_t> waits;
    tids.reserve(received);
    waits.reserve(received);
    for (size_t i = 0; i < received; i++) {
        int32_t& previous = previousTid[static_cast<size_t>(records[i].object)];
        if (previous != 0) {
            compared++;
            if (records[i].tid != previous) {
                threadChanges++;
            }
        }
        previous = records[i].tid;
        tids.push_back(records[i].tid);
        waits.push_back((records[i].receiveNs - records[i].sendNs) / kNanosPerMicro);
    }
    std::sort(tids.begin(), tids.end());
    const size_t distinctThreads =
            static_cast<size_t>(std::unique(tids.begin(), tids.end()) - tids.begin());
    std::sort(waits.begin(), waits.end());

    printf("received by a different thread than the previous one: %.1f%% (%zu of %zu), "
           "threads used: %zu\n",
           compared > 0 ? static_cast<double>(kPercent * threadChanges) / compared : 0,
           threadChanges, compared, distinctThreads);
    printf("send to receive, us: median %" PRId64 ", 99th percentile %" PRId64 ", max %" PRId64
           "\n",
           waits[received / 2], waits[received * kHighPercentile / kPercent], waits.back());
}

// Prints the CPU time and the sleeps of a process per transaction. A thread that is woken for
// every transaction shows up as one sleep per transaction.
void printUsage(const char* who, const Usage& usage, size_t transactions) {
    printf("%s: %.1f us of CPU and %.2f sleeps per transaction\n", who,
           static_cast<double>(usage.cpuNs) / kNanosPerMicro / transactions,
           static_cast<double>(usage.sleeps) / transactions);
}

// Asks the server for its usage over the run. Returns false if it does not answer.
bool collectServerUsage(Shared* shared) {
    shared->finished.store(true, std::memory_order_release);
    const int64_t deadlineNs = nowNs() + kStallTimeoutNs;
    while (!shared->serverUsageReady.load(std::memory_order_acquire)) {
        if (nowNs() > deadlineNs) {
            return false;
        }
        usleep(kPollSleepUs);
    }
    return true;
}

void report(const Options& options, const Shared* shared, const SendResult& send,
            const Usage& clientUsage) {
    const size_t received = static_cast<size_t>(
            std::min<uint64_t>(shared->received.load(std::memory_order_acquire), shared->capacity));
    const Record* records = shared->records;

    printf("sent: %zu, rejected: %zu, ", send.sent, send.rejected);
    printElapsed(send.sent, send.endNs - send.startNs);
    if (received < 2) {
        printf("received: %zu, not enough to report\n", received);
        return;
    }
    // The rate is taken between the first and the last transaction received. Handlers of different
    // objects can reserve their records in another order than they started, so compare the times.
    const auto [first, last] =
            std::minmax_element(records, records + received, [](const Record& a, const Record& b) {
                return a.receiveNs < b.receiveNs;
            });
    printf("received: %zu, ", received);
    printElapsed(received - 1, last->receiveNs - first->receiveNs);
    printDelivery(records, received, options.objects);
    printf("out of order: %" PRIu64 ", overlapping for one object: %" PRIu64 "\n",
           shared->outOfOrder.load(), shared->overlapped.load());
    printUsage("server", shared->serverUsage, received);
    printUsage("client", clientUsage, received);
    if (options.pageOutMs > 0) {
        printf("server: %" PRId64 " major page faults\n", shared->serverUsage.majorFaults);
    }
}

void usage() {
    fprintf(stderr,
            "Usage: binderOnewayThroughputTest [OPTIONS]\n"
            "\t-n N : Number of oneway transactions to send (default 100000).\n"
            "\t-t N : Number of looper threads in the server (default 16).\n"
            "\t-s N : Payload size in bytes (default 140).\n"
            "\t-q N : Maximum number of transactions sent but not yet received. The run fails if\n"
            "\t       a transaction is rejected. 0 sends without waiting for the server, so that\n"
            "\t       transactions are rejected once the binder buffer of the server is full, and\n"
            "\t       rejections do not fail the run (default 1000).\n"
            "\t-r N : Send at a fixed rate of N transactions per second (default 0, no limit).\n"
            "\t-w N : Give the handler computation that takes N microseconds on the core that\n"
            "\t       measures it at the start, and longer on a slower core (default 0).\n"
            "\t-i N : Give the handler N iterations of that computation instead, without\n"
            "\t       measuring, so that every run does the same work (default 0, use -w).\n"
            "\t-k N : Print how many iterations take N microseconds on the server cores, and\n"
            "\t       exit.\n"
            "\t-c MASK : Run the server on the cores in this hexadecimal bit mask, and the client\n"
            "\t       on the other cores. The handler computation is measured on the server's\n"
            "\t       cores (default 0, no pinning).\n"
            "\t-o N : Number of binder objects in the server. The client sends to them in turn\n"
            "\t       (default 1).\n"
            "\t-p N : Make the server move its own memory to swap every N milliseconds, so that\n"
            "\t       its threads take major page faults (default 0, never).\n"
            "\t-y N : 1 sends synchronous transactions instead, and waits for each reply, so that\n"
            "\t       one is outstanding at a time (default 0, oneway).\n");
}

// Parses an unsigned number in the given base, 10 or 16. Signs, prefixes, trailing characters and
// values above kMaxOptionValue are rejected.
bool parseCount(const char* text, int base, size_t* count) {
    const unsigned char first = static_cast<unsigned char>(text[0]);
    if (base == 16 ? !isxdigit(first) : !isdigit(first)) {
        return false;
    }
    errno = 0;
    char* end = nullptr;
    const unsigned long long value = strtoull(text, &end, base);
    if (errno != 0 || *end != '\0' || value > kMaxOptionValue) {
        return false;
    }
    *count = static_cast<size_t>(value);
    return true;
}

struct Flag {
    const char* name;
    size_t Options::* field;
    int base = 10;
};

constexpr Flag kFlags[] = {
        {"-n", &Options::calls},           {"-t", &Options::threads}, {"-s", &Options::payloadSize},
        {"-q", &Options::maxOutstanding},  {"-r", &Options::rate},    {"-w", &Options::workUs},
        {"-c", &Options::serverCores, 16}, {"-o", &Options::objects}, {"-p", &Options::pageOutMs},
        {"-i", &Options::iterations},      {"-k", &Options::calibrateUs},
        {"-y", &Options::sync},
};

bool parseOptions(int argc, char* argv[], Options* options) {
    // Every option takes a value.
    for (int i = 1; i < argc; i += 2) {
        const Flag* flag = std::find_if(std::begin(kFlags), std::end(kFlags), [&](const Flag& f) {
            return strcmp(f.name, argv[i]) == 0;
        });
        if (flag == std::end(kFlags) || i + 1 >= argc ||
            !parseCount(argv[i + 1], flag->base, &(options->*flag->field))) {
            return false;
        }
    }
    return options->calls > 0 && options->threads > 0 && options->threads <= kMaxThreads &&
            options->objects > 0 && options->objects <= kMaxObjects && options->sync <= 1;
}

// Maps the memory that the client and the server share, with room for one record per transaction.
// Returns nullptr on failure.
Shared* mapShared(size_t calls) {
    size_t size;
    if (__builtin_mul_overflow(calls, sizeof(Record), &size) ||
        __builtin_add_overflow(size, sizeof(Shared), &size)) {
        fprintf(stderr, "Too many transactions to record.\n");
        return nullptr;
    }
    void* mapping = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (mapping == MAP_FAILED) {
        perror("mmap");
        return nullptr;
    }
    // Touch the records now, so that page faults are not part of the measurement.
    memset(mapping, 0, size);
    Shared* shared = new (mapping) Shared;
    shared->serverReady.store(false);
    shared->started.store(0);
    shared->received.store(0);
    shared->handled.store(0);
    shared->outOfOrder.store(0);
    shared->overlapped.store(0);
    shared->finished.store(false);
    shared->serverUsageReady.store(false);
    shared->capacity = calls;
    return shared;
}

// Restricts this thread, and the threads and processes that it starts later, to the cores in
// `allowed` that `mask` selects: those in the mask, or with `inMask` false those outside it.
bool restrictToCores(const cpu_set_t& allowed, size_t mask, bool inMask) {
    cpu_set_t cores;
    CPU_ZERO(&cores);
    for (size_t core = 0; core < sizeof(mask) * 8; core++) {
        if (CPU_ISSET(core, &allowed) && ((mask >> core & 1) != 0) == inMask) {
            CPU_SET(core, &cores);
        }
    }
    if (CPU_COUNT(&cores) == 0) {
        fprintf(stderr, "No core left for the %s with server cores %zx.\n",
                inMask ? "server" : "client", mask);
        return false;
    }
    if (sched_setaffinity(0, sizeof(cores), &cores) != 0) {
        perror("sched_setaffinity");
        return false;
    }
    return true;
}

// Forks the server and waits until its loopers are idle. Returns its pid, or -1 on failure.
pid_t startServer(const Options& options, uint64_t workIterations, Shared* shared,
                  const std::string& name) {
    // Fork before this process opens the binder driver.
    const pid_t server = fork();
    if (server == -1) {
        perror("fork");
        return -1;
    }
    if (server == 0) {
        runServer(options, workIterations, shared, name);
    }
    while (!shared->serverReady.load(std::memory_order_acquire)) {
        if (waitpid(server, nullptr, WNOHANG) == server) {
            return -1;
        }
        usleep(kPollSleepUs);
    }
    return server;
}

// Sends the transactions once and reports how the server received them. Returns whether the run
// counts as passed.
bool measure(const Options& options, Shared* shared, const std::vector<sp<IBinder>>& sinks) {
    const Usage before = usageOfThisProcess();
    const SendResult send = runClient(options, shared, sinks);
    // The server usage then also covers the work of the last handler.
    const bool drained = !send.stalled && waitForCount(shared->handled, send.sent, kPollSleepUs);
    const Usage clientUsage = usageOfThisProcess() - before;
    const bool answered = collectServerUsage(shared);
    report(options, shared, send, clientUsage);

    if (!drained || !answered) {
        fprintf(stderr, "The server stopped receiving transactions.\n");
        return false;
    }
    if (shared->outOfOrder.load() > 0 || shared->overlapped.load() > 0) {
        fprintf(stderr,
                "Transactions for one binder object were not delivered in order and one "
                "at a time.\n");
        return false;
    }
    if (options.maxOutstanding > 0 && send.rejected > 0) {
        fprintf(stderr, "Transactions were rejected with at most %zu outstanding.\n",
                options.maxOutstanding);
        return false;
    }
    return true;
}

} // namespace

int main(int argc, char* argv[]) {
    Options options;
    if (!parseOptions(argc, argv, &options)) {
        usage();
        return EXIT_FAILURE;
    }
    Shared* shared = mapShared(options.calls);
    if (shared == nullptr) {
        return EXIT_FAILURE;
    }
    // With server cores given, the handler computation is measured on them and the server is
    // forked there. The client then moves to the other cores, so that it does not compete with
    // the server that it measures.
    const bool pinned = options.serverCores != 0;
    cpu_set_t allowed;
    if (pinned &&
        (sched_getaffinity(0, sizeof(allowed), &allowed) != 0 ||
         !restrictToCores(allowed, options.serverCores, true))) {
        return EXIT_FAILURE;
    }
    if (options.calibrateUs > 0) {
        printf("iterations for %zu us: %" PRIu64 "\n", options.calibrateUs,
               options.calibrateUs * iterationsPerMicro());
        return EXIT_SUCCESS;
    }
    const uint64_t workIterations = handlerIterations(options);
    const std::string name = "binderOnewayThroughputTest." + std::to_string(getpid());
    const pid_t server = startServer(options, workIterations, shared, name);
    if (server == -1) {
        return EXIT_FAILURE;
    }
    if (pinned && !restrictToCores(allowed, options.serverCores, false)) {
        kill(server, SIGKILL);
        return EXIT_FAILURE;
    }

    std::vector<sp<IBinder>> sinks;
    for (size_t object = 0; object < options.objects; object++) {
        sinks.push_back(defaultServiceManager()->waitForService(objectName(name, object)));
        if (sinks.back() == nullptr) {
            fprintf(stderr, "Unable to find the service.\n");
            kill(server, SIGKILL);
            return EXIT_FAILURE;
        }
    }

    printSettings(options, workIterations);
    const bool passed = measure(options, shared, sinks);

    kill(server, SIGKILL);
    waitpid(server, nullptr, 0);
    return passed ? EXIT_SUCCESS : EXIT_FAILURE;
}
