/*
 * Tests for how the driver delivers oneway transactions that are queued for one binder object.
 *
 * The driver delivers them one at a time and releases the next one when the receiver frees the
 * buffer of the previous one. These tests cover what the receiver can observe when that happens:
 * which thread gets the next transaction, whether anything else is left to read, and whether
 * other work for the process still gets its turn.
 */

#include <binder/Binder.h>
#include <binder/IBinder.h>
#include <binder/IPCThreadState.h>
#include <binder/IServiceManager.h>
#include <binder/Parcel.h>
#include <binder/ProcessState.h>
#include <gtest/gtest.h>
#include <utils/String16.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <linux/android/binder.h>
#include <linux/userfaultfd.h>
#include <poll.h>
#include <sched.h>
#include <signal.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

using namespace android;

namespace {

enum Code : uint32_t {
    ONEWAY = IBinder::FIRST_CALL_TRANSACTION,
    PING,
    REPORT,
    CREATE_SERVICE,
    // Links to the death of the binder in the request, see QueueService::linkToDeathOf.
    LINK_TO_DEATH,
};

// What the handler of a oneway call does besides recording the call.
enum class Action : int32_t {
    kNone,
    // Frees the incoming buffer, which releases the next queued call, and then keeps running.
    kFreeThenStayBusy,
    // Frees the incoming buffer and makes an outgoing oneway call before returning.
    kFreeThenSendOneway,
    // The same, with a thread waiting for work that reads into a buffer of kSmallReadSize.
    kFreeForSmallReaderThenStayBusy,
    // The same, with both the small reader and a separate thread waiting in poll().
    kFreeForSmallReaderAndPollThenStayBusy,
    // Not recorded as a call. Starts the thread that takes the next call, see
    // QueueService::startDelayedReader.
    kStartDelayedReader,
    // Writes to the file descriptor in the request when the call reaches its handler.
    kNotifyWhenHandled,
    // Starts a thread that waits for work and counts the reads that return with nothing to do,
    // then returns, so that libbinder frees the buffer and reads in one ioctl. See
    // QueueService::startCountingReader.
    kStartCountingReader,
    // Starts a looper that the driver wakes first and keeps it from running, then frees the
    // buffer and stays busy. See QueueService::holdWokenLooperThenFree.
    kHoldWokenLooperThenFree,
};

// Whether the server could hold a thread between freeing a buffer and reading.
enum class DelayedReader : int32_t {
    kNotAsked,
    // The page fault that holds the thread could not be set up.
    kUnsupported,
    kStarted,
};

// Whether the server could keep the looper that the driver wakes first from running.
enum class HeldLooper : int32_t {
    // CPU pinning or SCHED_FIFO is not available.
    kUnsupported,
    kHeld,
};

constexpr char kServerFlag[] = "--server";
constexpr char kOneLooper[] = "one-looper";
constexpr char kTwoLoopers[] = "two-loopers";
constexpr char kPolling[] = "polling";

constexpr int64_t kNanosPerMilli = 1000000;
constexpr int64_t kStartTimeoutNs = 5000 * kNanosPerMilli;
constexpr int64_t kReportTimeoutNs = 10000 * kNanosPerMilli;

// Work done by every oneway handler, so that a sender can outpace the server.
constexpr useconds_t kHandlerUs = 500;
// Time for a second call to arrive and be queued behind the one that is running.
constexpr useconds_t kParkUs = 100 * 1000;
// How long a handler keeps its thread busy after freeing its buffer.
constexpr useconds_t kBusyUs = 1000 * 1000;
// Generous liveness bound for an idle thread to start the next call after the free. The ordering
// checks against the delayed reader are the primary requirement; this is not a benchmark limit.
constexpr int64_t kWakeBoundNs = 500 * kNanosPerMilli;

// How long the delayed reader is held between freeing a buffer and reading.
constexpr useconds_t kFaultHoldUs = 300 * 1000;
// How long after its page fault the delayed reader gets to return from the driver on its own.
constexpr useconds_t kReaderReturnUs = 50 * 1000;

// How long the counting reader waits for work before it is interrupted.
constexpr useconds_t kCountingUs = 3 * kParkUs;

// How long the looper that the driver wakes first is kept from running.
constexpr useconds_t kHoldUs = 500 * 1000;
// Work that does not depend on the held looper has to start well before the hold ends.
constexpr int64_t kHeldBoundNs = static_cast<int64_t>(kHoldUs) * 1000 / 2;

// The smallest read buffer for which the driver waits for work. A transaction fits only if it is
// the first thing the driver writes after the BR_NOOP that starts every read.
constexpr size_t kSmallReadSize = 2 * sizeof(uint32_t) + sizeof(binder_transaction_data_secctx);

// Number of oneway calls sent to the polling server in one burst.
constexpr int32_t kBurst = 200;
// Exit status of the polling server when its binder fd is readable with nothing sent.
constexpr int kLeftoverExit = 3;

// Number of oneway calls sent to a server that is about to be killed or is frozen.
constexpr int32_t kBacklog = 200;
// How soon the death of the server must be reported.
constexpr int64_t kDeathBoundNs = 2000 * kNanosPerMilli;
// How often a server is killed while it is being flooded, and how long each flood runs before.
constexpr int kKillRounds = 20;
constexpr useconds_t kFloodBeforeKillUs = 50 * 1000;
// How long the driver may wait for the server to finish its transactions before freezing it.
constexpr uint32_t kFreezeTimeoutMs = 1000;
// Root of the cgroup hierarchy that has the freezer.
constexpr char kCgroupRoot[] = "/sys/fs/cgroup";

// The flood sends twice as fast as the server handles calls and outlasts kPingBoundNs.
constexpr useconds_t kFloodGapUs = kHandlerUs / 2;
constexpr int64_t kFloodNs = 2000 * kNanosPerMilli;
constexpr int64_t kPingBoundNs = 1000 * kNanosPerMilli;

int64_t nowNs() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000 * kNanosPerMilli + ts.tv_nsec;
}

// Whether the thread is sleeping. For a thread whose next step is to read from the driver, that
// means it is waiting for work.
bool isAsleep(pid_t tid) {
    std::ifstream stat("/proc/self/task/" + std::to_string(tid) + "/stat");
    std::string pid, name, state;
    stat >> pid >> name >> state;
    return state == "S";
}

bool writeFile(const std::string& path, const std::string& value) {
    std::ofstream file(path);
    file << value;
    file.flush();
    return file.good();
}

// One oneway call as seen by the server.
struct Call {
    int32_t seq;
    int32_t tid;
    int64_t startNs;
};

struct Report {
    // Whether a oneway handler started while another one had not released its buffer yet.
    bool overlapped = false;
    // Whether released work reentered the freeing thread during its outgoing oneway call.
    bool reenteredDuringOutgoingCall = false;
    bool outgoingCallStarted = false;
    int32_t outgoingCallStatus = UNKNOWN_ERROR;
    // When a handler freed its buffer early, see Action::kFreeThenStayBusy.
    int64_t freedNs = 0;
    DelayedReader delayedReader = DelayedReader::kNotAsked;
    // When the delayed reader was let go, and how many calls had been started by then.
    int64_t resumedNs = 0;
    int32_t callsBeforeResume = 0;
    // Reads by the counting reader that returned with nothing but BR_NOOP.
    int32_t emptyReads = 0;
    // When the looper that the driver woke first was let run again.
    int64_t holdEndNs = 0;
    // When the binder that the server linked to died, as reported to the server.
    int64_t deathNs = 0;
    std::vector<Call> calls;
};

// Frees a transaction buffer with a write that does not read.
void freeBuffer(int fd, binder_uintptr_t buffer) {
    struct {
        uint32_t command;
        binder_uintptr_t buffer;
    } __attribute__((packed)) command = {BC_FREE_BUFFER, buffer};
    binder_write_read request = {};
    request.write_size = sizeof(command);
    request.write_buffer = reinterpret_cast<binder_uintptr_t>(&command);
    ioctl(fd, BINDER_WRITE_READ, &request);
}

void notifyDelayedReader(const binder::unique_fd& fd, DelayedReader state) {
    if (fd.ok()) {
        const auto value = static_cast<int32_t>(state);
        (void)TEMP_FAILURE_RETRY(write(fd.get(), &value, sizeof(value)));
    }
}

void writeValue(const binder::unique_fd& fd, int32_t value) {
    (void)TEMP_FAILURE_RETRY(write(fd.get(), &value, sizeof(value)));
}

// Makes the driver flush the binder file, as every close of a descriptor for it does. The driver
// handles the flush on a workqueue, after this returns.
void flushBinder(int fd) {
    close(dup(fd));
}

cpu_set_t onlyCpu(int cpu) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    return set;
}

class DeathTime : public IBinder::DeathRecipient {
public:
    void binderDied(const wp<IBinder>&) override { diedNs = nowNs(); }

    std::atomic<int64_t> diedNs = 0;
};

class QueueService : public BBinder {
public:
    status_t onTransact(uint32_t code, const Parcel& data, Parcel* reply, uint32_t flags) override {
        switch (code) {
            case ONEWAY:
                handleOneway(data);
                return OK;
            case PING:
                return OK;
            case REPORT:
                writeReport(reply);
                return OK;
            case CREATE_SERVICE:
                return reply->writeStrongBinder(sp<QueueService>::make());
            case LINK_TO_DEATH:
                return linkToDeathOf(data.readStrongBinder());
            default:
                return BBinder::onTransact(code, data, reply, flags);
        }
    }

    size_t count() {
        std::lock_guard<std::mutex> lock(mLock);
        return mReport.calls.size();
    }

private:
    void startCall(int32_t seq) {
        const bool overlapped = mHoldsBuffer.exchange(true);
        const int32_t tid = gettid();
        const bool reentered = mOutgoingCallActive.load(std::memory_order_acquire) &&
                mOutgoingCallTid.load(std::memory_order_relaxed) == tid;
        std::lock_guard<std::mutex> lock(mLock);
        mReport.overlapped |= overlapped;
        mReport.reenteredDuringOutgoingCall |= reentered;
        mReport.calls.push_back({seq, tid, nowNs()});
    }

    void handleOneway(const Parcel& data) {
        const int32_t seq = data.readInt32();
        const Action action = static_cast<Action>(data.readInt32());
        if (action == Action::kStartDelayedReader) {
            startDelayedReader(data);
            return;
        }
        startCall(seq);

        if (action == Action::kStartCountingReader) {
            startCountingReader(data);
            // Time for the next call to be queued behind this one.
            usleep(kParkUs);
            mHoldsBuffer = false;
            return;
        }
        if (action == Action::kHoldWokenLooperThenFree) {
            holdWokenLooperThenFree(data);
            return;
        }

        sp<IBinder> outgoingTarget;
        if (action == Action::kFreeThenSendOneway) {
            outgoingTarget = data.readStrongBinder();
        }

        if (action == Action::kNotifyWhenHandled) {
            binder::unique_fd notification;
            if (data.readUniqueFileDescriptor(&notification) == OK) {
                const int32_t handled = 1;
                (void)TEMP_FAILURE_RETRY(write(notification.get(), &handled, sizeof(handled)));
            }
            mHoldsBuffer = false;
            return;
        }

        if (action != Action::kNone) {
            usleep(kParkUs);
            if (action == Action::kFreeForSmallReaderAndPollThenStayBusy) {
                startPollReader();
                startSmallReader();
            } else if (action == Action::kFreeForSmallReaderThenStayBusy) {
                startSmallReader();
            }
            {
                std::lock_guard<std::mutex> lock(mLock);
                mReport.freedNs = nowNs();
            }
            // The driver may deliver the next call as soon as this buffer is freed.
            mHoldsBuffer = false;
            const_cast<Parcel&>(data).freeData();
            IPCThreadState::self()->flushCommands();
            if (action == Action::kFreeThenSendOneway) {
                sendOnewayWhileReleasedWorkIsQueued(outgoingTarget);
                return;
            }
            usleep(kBusyUs);
            return;
        }

        usleep(kHandlerUs);
        mHoldsBuffer = false;
    }

    void sendOnewayWhileReleasedWorkIsQueued(const sp<IBinder>& target) {
        const int32_t tid = gettid();
        {
            std::lock_guard<std::mutex> lock(mLock);
            mReport.outgoingCallStarted = true;
        }

        mOutgoingCallTid.store(tid, std::memory_order_relaxed);
        mOutgoingCallActive.store(true, std::memory_order_release);
        Parcel data;
        const status_t status = target != nullptr
                ? target->transact(PING, data, nullptr, IBinder::FLAG_ONEWAY)
                : BAD_VALUE;
        mOutgoingCallActive.store(false, std::memory_order_release);

        std::lock_guard<std::mutex> lock(mLock);
        mReport.outgoingCallStatus = status;
    }

    // Starts a thread that reads from the driver once, and returns when it is waiting for work.
    // It registered last, so it is the thread the driver wakes first.
    void startSmallReader() {
        std::atomic<pid_t> tid = 0;
        std::thread([this, &tid] {
            int fd = -1;
            // Also makes this thread a looper.
            IPCThreadState::self()->setupPolling(&fd);

            alignas(binder_transaction_data) uint8_t buffer[kSmallReadSize];
            binder_write_read request = {};
            request.read_size = sizeof(buffer);
            request.read_buffer = reinterpret_cast<binder_uintptr_t>(buffer);
            tid = gettid();
            if (ioctl(fd, BINDER_WRITE_READ, &request) == 0) {
                handleSmallRead(fd, buffer, request.read_consumed);
            }
        }).detach();

        while (tid == 0 || !isAsleep(tid)) {
            usleep(1000);
        }
    }

    // Starts a looper that waits for process work through poll(), without also becoming a normal
    // blocking reader. This deliberately mixes the two driver interfaces to exercise the fallback
    // that C Binder provides when no normal reader can take queued process work.
    void startPollReader() {
        std::atomic<pid_t> tid = 0;
        std::thread([&tid] {
            int fd = -1;
            if (IPCThreadState::self()->setupPolling(&fd) != OK) {
                return;
            }
            tid = gettid();

            struct pollfd binder = {.fd = fd, .events = POLLIN, .revents = 0};
            if (TEMP_FAILURE_RETRY(poll(&binder, 1, -1)) == 1 && binder.revents & POLLIN) {
                IPCThreadState::self()->handlePolledCommands();
            }
            for (;;) {
                pause();
            }
        }).detach();

        while (tid == 0 || !isAsleep(tid)) {
            usleep(1000);
        }
    }

    // Records the oneway call that the driver delivered into `buffer`, if any. Returns the
    // transaction buffer of that call, which the caller has to free, or 0.
    binder_uintptr_t startDeliveredCall(const uint8_t* buffer, size_t size) {
        uint32_t command = 0;
        for (size_t at = 0; at + sizeof(command) <= size; at += _IOC_SIZE(command)) {
            memcpy(&command, buffer + at, sizeof(command));
            at += sizeof(command);
            if (command != BR_TRANSACTION) {
                continue;
            }

            binder_transaction_data transaction;
            memcpy(&transaction, buffer + at, sizeof(transaction));
            int32_t seq = 0;
            memcpy(&seq, reinterpret_cast<const void*>(transaction.data.ptr.buffer), sizeof(seq));
            startCall(seq);
            return transaction.data.ptr.buffer;
        }
        return 0;
    }

    // Handles a oneway call if the driver delivered one, as a looper would.
    void handleSmallRead(int fd, const uint8_t* buffer, size_t size) {
        const binder_uintptr_t held = startDeliveredCall(buffer, size);
        if (held != 0) {
            mHoldsBuffer = false;
            freeBuffer(fd, held);
        }
    }

    // Starts a looper that reads from the driver and counts the reads that return with nothing but
    // the BR_NOOP that starts every read. Returns once it is waiting for work, so that it is the
    // thread the driver wakes for the call that this handler releases. After kCountingUs a second
    // thread interrupts it, so that it is not given the call that fetches the report, and then
    // writes to the file descriptor in the request.
    void startCountingReader(const Parcel& data) {
        binder::unique_fd notification;
        if (data.readUniqueFileDescriptor(&notification) != OK) {
            return;
        }
        std::promise<pid_t> started;
        std::future<pid_t> reader = started.get_future();
        std::thread([this, started = std::move(started)]() mutable {
            countReads(std::move(started));
        }).detach();
        const pid_t tid = reader.get();
        while (!isAsleep(tid)) {
            usleep(1000);
        }

        std::thread([this, tid, notification = std::move(notification)] {
            usleep(kCountingUs);
            mStopCounting = true;
            while (!mCountingReturned) {
                tgkill(getpid(), tid, SIGUSR1);
                usleep(kReaderReturnUs);
            }
            writeValue(notification, 1);
        }).detach();
    }

    void countReads(std::promise<pid_t> started) {
        int fd = -1;
        // Also makes this thread a looper.
        IPCThreadState::self()->setupPolling(&fd);

        alignas(binder_transaction_data) uint8_t buffer[256];
        started.set_value(gettid());
        while (!mStopCounting) {
            binder_write_read request = {};
            request.read_size = sizeof(buffer);
            request.read_buffer = reinterpret_cast<binder_uintptr_t>(buffer);
            if (ioctl(fd, BINDER_WRITE_READ, &request) != 0) {
                continue;
            }
            const binder_uintptr_t held = startDeliveredCall(buffer, request.read_consumed);
            if (held != 0) {
                mHoldsBuffer = false;
                freeBuffer(fd, held);
            } else if (request.read_consumed == sizeof(uint32_t)) {
                std::lock_guard<std::mutex> lock(mLock);
                mReport.emptyReads++;
            }
        }
        mCountingReturned = true;
        // Does not read again, so that it is never given a call that needs a reply.
        for (;;) {
            pause();
        }
    }

    // Starts a looper that registers for work after the idle loopers, so that the driver wakes it
    // first for the call that this handler releases, and keeps it from running for kHoldUs: it is
    // pinned to one CPU, which a SCHED_FIFO thread then occupies. Then frees the buffer, which
    // releases the next call, reports through the file descriptor in the request and stays busy.
    void holdWokenLooperThenFree(const Parcel& data) {
        binder::unique_fd notification;
        if (data.readUniqueFileDescriptor(&notification) != OK) {
            mHoldsBuffer = false;
            return;
        }
        const int cpu = startPinnedLooper();
        // Time for the next call to be queued behind this one.
        usleep(kParkUs);
        if (cpu < 0 || !holdCpu(cpu)) {
            mHoldsBuffer = false;
            writeValue(notification, static_cast<int32_t>(HeldLooper::kUnsupported));
            return;
        }

        {
            std::lock_guard<std::mutex> lock(mLock);
            mReport.freedNs = nowNs();
        }
        mHoldsBuffer = false;
        const_cast<Parcel&>(data).freeData();
        IPCThreadState::self()->flushCommands();
        writeValue(notification, static_cast<int32_t>(HeldLooper::kHeld));
        usleep(kBusyUs);
    }

    // Starts a looper pinned to the last CPU this thread may use, and returns that CPU once the
    // looper is waiting for work, or -1.
    int startPinnedLooper() {
        cpu_set_t allowed;
        CPU_ZERO(&allowed);
        if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0 || CPU_COUNT(&allowed) < 2) {
            return -1;
        }
        int cpu = CPU_SETSIZE - 1;
        while (!CPU_ISSET(cpu, &allowed)) {
            cpu--;
        }

        std::promise<pid_t> started;
        std::future<pid_t> looper = started.get_future();
        std::thread([cpu, started = std::move(started)]() mutable {
            const cpu_set_t pinned = onlyCpu(cpu);
            if (sched_setaffinity(0, sizeof(pinned), &pinned) != 0) {
                started.set_value(0);
                return;
            }
            started.set_value(gettid());
            IPCThreadState::self()->joinThreadPool(true);
        }).detach();
        const pid_t tid = looper.get();
        if (tid == 0) {
            return -1;
        }
        while (!isAsleep(tid)) {
            usleep(1000);
        }
        return cpu;
    }

    // Occupies the CPU with a SCHED_FIFO thread for kHoldUs. Returns false if that is not allowed.
    bool holdCpu(int cpu) {
        std::promise<bool> started;
        std::future<bool> holding = started.get_future();
        std::thread([this, cpu, started = std::move(started)]() mutable {
            const cpu_set_t held = onlyCpu(cpu);
            const struct sched_param param = {.sched_priority = 1};
            if (sched_setaffinity(0, sizeof(held), &held) != 0 ||
                sched_setscheduler(0, SCHED_FIFO, &param) != 0) {
                started.set_value(false);
                return;
            }
            const int64_t endNs = nowNs() + static_cast<int64_t>(kHoldUs) * 1000;
            started.set_value(true);
            while (nowNs() < endNs) {
            }
            std::lock_guard<std::mutex> lock(mLock);
            mReport.holdEndNs = nowNs();
        }).detach();
        return holding.get();
    }

    status_t linkToDeathOf(const sp<IBinder>& target) {
        if (target == nullptr) {
            return BAD_VALUE;
        }
        mDeathTarget = target;
        return target->linkToDeath(mDeathTime);
    }

    // Creates a page that is not present and a userfaultfd that reports the first access to it.
    // An access from inside the kernel, such as the driver writing to a read buffer, then blocks
    // until the fault is resolved through the userfaultfd.
    bool createMissingPage() {
        mPageSize = static_cast<size_t>(sysconf(_SC_PAGESIZE));
        mUffd = static_cast<int>(syscall(__NR_userfaultfd, O_CLOEXEC));
        struct uffdio_api api = {.api = UFFD_API, .features = 0, .ioctls = 0};
        if (mUffd == -1 || ioctl(mUffd, UFFDIO_API, &api) != 0) {
            return false;
        }
        mMissingPage = mmap(nullptr, mPageSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS,
                            -1, 0);
        if (mMissingPage == MAP_FAILED) {
            return false;
        }
        struct uffdio_register registration = {};
        registration.range.start = reinterpret_cast<uintptr_t>(mMissingPage);
        registration.range.len = mPageSize;
        registration.mode = UFFDIO_REGISTER_MODE_MISSING;
        return ioctl(mUffd, UFFDIO_REGISTER, &registration) == 0;
    }

    // Starts a thread that takes the next call and then does what a looper does at the end of a
    // handler in one ioctl: it frees the buffer of the call, which releases the call queued
    // behind it, and reads. Its read buffer is a page that is not present. Every read starts by
    // writing BR_NOOP to the buffer, so the thread is held in a page fault after the free and
    // before it can take the released call. A second thread resolves the fault after
    // kFaultHoldUs.
    //
    // Runs in the handler of the only looper. That handler frees its own buffer and stays out of
    // the driver until the delayed reader has its call, so that the reader is the only thread
    // waiting for work when the call arrives. Afterwards the looper is idle.
    void startDelayedReader(const Parcel& data) {
        binder::unique_fd notification;
        if (data.dataAvail() != 0 && data.readUniqueFileDescriptor(&notification) != OK) {
            return;
        }
        const bool supported = createMissingPage();
        {
            std::lock_guard<std::mutex> lock(mLock);
            mReport.delayedReader =
                    supported ? DelayedReader::kStarted : DelayedReader::kUnsupported;
        }
        if (!supported) {
            notifyDelayedReader(notification, DelayedReader::kUnsupported);
            return;
        }

        std::thread([this, notification = std::move(notification)] {
            resolveFaultLater(notification);
        }).detach();
        std::thread([this] { readDelayed(); }).detach();
        while (mReaderTid == 0 || !isAsleep(mReaderTid)) {
            usleep(1000);
        }

        const_cast<Parcel&>(data).freeData();
        IPCThreadState::self()->flushCommands();
        while (count() == 0) {
            usleep(1000);
        }
    }

    void readDelayed() {
        int fd = -1;
        // Also makes this thread a looper.
        IPCThreadState::self()->setupPolling(&fd);

        alignas(binder_transaction_data) uint8_t buffer[256];
        binder_uintptr_t held = 0;
        mReaderTid = gettid();
        while (held == 0) {
            binder_write_read request = {};
            request.read_size = sizeof(buffer);
            request.read_buffer = reinterpret_cast<binder_uintptr_t>(buffer);
            if (ioctl(fd, BINDER_WRITE_READ, &request) != 0) {
                return;
            }
            held = startDeliveredCall(buffer, request.read_consumed);
        }

        // Time for the next call to be queued behind this one and for the looper to go idle.
        usleep(kParkUs);
        {
            std::lock_guard<std::mutex> lock(mLock);
            mReport.freedNs = nowNs();
        }
        mHoldsBuffer = false;
        struct {
            uint32_t command;
            binder_uintptr_t buffer;
        } __attribute__((packed)) command = {BC_FREE_BUFFER, held};
        binder_write_read request = {};
        request.write_size = sizeof(command);
        request.write_buffer = reinterpret_cast<binder_uintptr_t>(&command);
        request.read_size = mPageSize;
        request.read_buffer = reinterpret_cast<binder_uintptr_t>(mMissingPage);
        const int result = ioctl(fd, BINDER_WRITE_READ, &request);
        mReaderReturned = true;

        if (result == 0) {
            handleSmallRead(fd, static_cast<const uint8_t*>(mMissingPage), request.read_consumed);
        }
        // Does not read again, so that it is never given a call that needs a reply.
        for (;;) {
            pause();
        }
    }

    void resolveFaultLater(const binder::unique_fd& notification) {
        struct uffd_msg message;
        if (read(mUffd, &message, sizeof(message)) != sizeof(message) ||
            message.event != UFFD_EVENT_PAGEFAULT) {
            return;
        }
        notifyDelayedReader(notification, DelayedReader::kStarted);
        usleep(kFaultHoldUs);
        {
            std::lock_guard<std::mutex> lock(mLock);
            mReport.callsBeforeResume = static_cast<int32_t>(mReport.calls.size());
            mReport.resumedNs = nowNs();
        }
        struct uffdio_zeropage zero = {};
        zero.range.start = reinterpret_cast<uintptr_t>(mMissingPage);
        zero.range.len = mPageSize;
        ioctl(mUffd, UFFDIO_ZEROPAGE, &zero);

        // If another thread took the released call, the reader is now waiting for work in the
        // driver. Interrupt it, so that it is not given the call that fetches the report.
        usleep(kReaderReturnUs);
        while (!mReaderReturned) {
            tgkill(getpid(), mReaderTid, SIGUSR1);
            usleep(kReaderReturnUs);
        }
    }

    void writeReport(Parcel* reply) {
        std::lock_guard<std::mutex> lock(mLock);
        reply->writeBool(mReport.overlapped);
        reply->writeBool(mReport.reenteredDuringOutgoingCall);
        reply->writeBool(mReport.outgoingCallStarted);
        reply->writeInt32(mReport.outgoingCallStatus);
        reply->writeInt64(mReport.freedNs);
        reply->writeInt32(static_cast<int32_t>(mReport.delayedReader));
        reply->writeInt64(mReport.resumedNs);
        reply->writeInt32(mReport.callsBeforeResume);
        reply->writeInt32(mReport.emptyReads);
        reply->writeInt64(mReport.holdEndNs);
        reply->writeInt64(mDeathTime->diedNs);
        reply->writeInt32(static_cast<int32_t>(mReport.calls.size()));
        for (const Call& call : mReport.calls) {
            reply->writeInt32(call.seq);
            reply->writeInt32(call.tid);
            reply->writeInt64(call.startNs);
        }
    }

    std::mutex mLock;
    Report mReport;
    std::atomic<bool> mHoldsBuffer = false;
    std::atomic<int32_t> mOutgoingCallTid = 0;
    std::atomic<bool> mOutgoingCallActive = false;

    // State of the delayed reader.
    int mUffd = -1;
    size_t mPageSize = 0;
    void* mMissingPage = nullptr;
    std::atomic<pid_t> mReaderTid = 0;
    std::atomic<bool> mReaderReturned = false;

    // State of the counting reader.
    std::atomic<bool> mStopCounting = false;
    std::atomic<bool> mCountingReturned = false;

    sp<IBinder> mDeathTarget;
    const sp<DeathTime> mDeathTime = sp<DeathTime>::make();
};

[[noreturn]] void servePolling(int fd, const sp<QueueService>& service) {
    for (;;) {
        struct pollfd binder = {.fd = fd, .events = POLLIN, .revents = 0};
        poll(&binder, 1, -1);
        IPCThreadState::self()->handlePolledCommands();

        // The client sends nothing for a while after the burst. If the fd is readable anyway, the
        // next read would block in the driver, so exit and let the client see a dead service.
        if (service->count() == static_cast<size_t>(kBurst) && poll(&binder, 1, 0) > 0) {
            _exit(kLeftoverExit);
        }
    }
}

int serve(const std::string& mode, const String16& name) {
    // Do not outlive the test.
    prctl(PR_SET_PDEATHSIG, SIGKILL);

    // Only the threads started here handle transactions.
    ProcessState::self()->setThreadPoolMaxThreadCount(0);

    // SIGUSR1 interrupts a thread that waits in the driver, see QueueService::resolveFaultLater.
    struct sigaction interrupt = {};
    interrupt.sa_handler = [](int) {};
    sigaction(SIGUSR1, &interrupt, nullptr);

    int fd = -1;
    if (mode == kPolling) {
        IPCThreadState::self()->setupPolling(&fd);
    }

    const sp<QueueService> service = sp<QueueService>::make();
    if (defaultServiceManager()->addService(name, service) != OK) {
        return EXIT_FAILURE;
    }

    if (mode == kPolling) {
        servePolling(fd, service);
    }
    if (mode == kTwoLoopers) {
        std::thread([] { IPCThreadState::self()->joinThreadPool(true); }).detach();
    }
    IPCThreadState::self()->joinThreadPool(true);
    return EXIT_FAILURE;
}

class DeathFlag : public IBinder::DeathRecipient {
public:
    void binderDied(const wp<IBinder>&) override { died = true; }

    std::atomic<bool> died = false;
};

class BinderOnewayQueueTest : public ::testing::Test {
protected:
    void TearDown() override {
        if (!mCgroup.empty()) {
            writeFile(mCgroup + "/cgroup.freeze", "0");
        }
        killServer();
        killProcess(&mOther);
        if (!mCgroup.empty()) {
            rmdir(mCgroup.c_str());
        }
    }

    void killServer() { killProcess(&mServer); }

    static void killProcess(pid_t* pid) {
        if (*pid > 0) {
            kill(*pid, SIGKILL);
            waitpid(*pid, nullptr, 0);
            *pid = -1;
        }
    }

    void startServer(const char* mode) {
        ASSERT_NO_FATAL_FAILURE(startProcess(mode, &mServer, &mService));
    }

    // Runs this binary again as a server. A process that already uses binder cannot fork a child
    // that uses it too, so the child has to start from exec.
    void startProcess(const char* mode, pid_t* pid, sp<IBinder>* service) {
        static int sStarted = 0;
        const std::string name = "binderOnewayQueueTest." + std::to_string(getpid()) + "." +
                std::to_string(sStarted++);

        *pid = fork();
        ASSERT_NE(-1, *pid);
        if (*pid == 0) {
            execl("/proc/self/exe", "binderOnewayQueueTest", kServerFlag, mode, name.c_str(),
                  nullptr);
            _exit(EXIT_FAILURE);
        }

        *service = nullptr;
        const int64_t deadlineNs = nowNs() + kStartTimeoutNs;
        while (*service == nullptr && nowNs() < deadlineNs) {
            usleep(kParkUs);
            *service = defaultServiceManager()->checkService(String16(name.c_str()));
        }
        ASSERT_NE(nullptr, *service) << "the " << mode << " server did not register";
    }

    // Sends call 0 with `action` and the write end of a new pipe, then call 1 with `next`, and
    // waits for the value that the server writes to the pipe.
    void sendAndWaitForValue(Action action, Action next, int32_t* value) {
        int pipeFds[2];
        ASSERT_EQ(0, pipe2(pipeFds, O_CLOEXEC));
        binder::unique_fd readEnd(pipeFds[0]);
        binder::unique_fd writeEnd(pipeFds[1]);
        ASSERT_EQ(OK, sendOnewayTo(mService, 0, action, writeEnd));
        writeEnd.reset();
        ASSERT_EQ(OK, sendOneway(1, next));

        struct pollfd wait = {.fd = readEnd.get(), .events = POLLIN, .revents = 0};
        ASSERT_EQ(1, TEMP_FAILURE_RETRY(poll(&wait, 1, kStartTimeoutNs / kNanosPerMilli)));
        ASSERT_TRUE(wait.revents & POLLIN) << "the server did not write to the pipe";
        ASSERT_EQ(static_cast<ssize_t>(sizeof(*value)),
                  TEMP_FAILURE_RETRY(read(readEnd.get(), value, sizeof(*value))));
    }

    // Sends a call whose handler starts a looper that the driver will wake first and keeps it from
    // running for kHoldUs, then frees the buffer, which releases a second call with `next`.
    // Returns once the buffer is freed. Sets `held` to false if the looper could not be held.
    void releaseWithWokenLooperHeld(Action next, bool* held) {
        int32_t state = 0;
        ASSERT_NO_FATAL_FAILURE(
                sendAndWaitForValue(Action::kHoldWokenLooperThenFree, next, &state));
        *held = state == static_cast<int32_t>(HeldLooper::kHeld);
    }

    status_t sendOneway(int32_t seq, Action action = Action::kNone) {
        return sendOnewayTo(mService, seq, action);
    }

    status_t sendOneway(int32_t seq, Action action, const sp<IBinder>& outgoingTarget) {
        Parcel data;
        data.writeInt32(seq);
        data.writeInt32(static_cast<int32_t>(action));
        const status_t status = data.writeStrongBinder(outgoingTarget);
        if (status != OK) {
            return status;
        }
        return mService->transact(ONEWAY, data, nullptr, IBinder::FLAG_ONEWAY);
    }

    status_t sendOnewayTo(const sp<IBinder>& service, int32_t seq, Action action,
                          const binder::unique_fd& notification = {}) {
        Parcel data;
        data.writeInt32(seq);
        data.writeInt32(static_cast<int32_t>(action));
        if (notification.ok()) {
            const status_t status = data.writeUniqueFileDescriptor(notification);
            if (status != OK) {
                return status;
            }
        }
        return service->transact(ONEWAY, data, nullptr, IBinder::FLAG_ONEWAY);
    }

    void readReport(const sp<IBinder>& service, Report* report) {
        Parcel data;
        Parcel reply;
        ASSERT_EQ(OK, service->transact(REPORT, data, &reply));

        report->overlapped = reply.readBool();
        report->reenteredDuringOutgoingCall = reply.readBool();
        report->outgoingCallStarted = reply.readBool();
        report->outgoingCallStatus = reply.readInt32();
        report->freedNs = reply.readInt64();
        report->delayedReader = static_cast<DelayedReader>(reply.readInt32());
        report->resumedNs = reply.readInt64();
        report->callsBeforeResume = reply.readInt32();
        report->emptyReads = reply.readInt32();
        report->holdEndNs = reply.readInt64();
        report->deathNs = reply.readInt64();
        report->calls.resize(static_cast<size_t>(reply.readInt32()));
        for (Call& call : report->calls) {
            call.seq = reply.readInt32();
            call.tid = reply.readInt32();
            call.startNs = reply.readInt64();
        }
    }

    // Reads the report once the server has started `count` oneway calls.
    void waitForCalls(size_t count, Report* report) {
        const int64_t deadlineNs = nowNs() + kReportTimeoutNs;
        do {
            ASSERT_NO_FATAL_FAILURE(readReport(mService, report));
            if (report->calls.size() >= count) {
                return;
            }
            usleep(kParkUs);
        } while (nowNs() < deadlineNs);
        FAIL() << "the server started " << report->calls.size() << " of " << count << " calls";
    }

    // Freezes the server the way the system freezes a cached app: binder first, then all of its
    // threads through the cgroup freezer. Returns false if either is not available.
    bool freezeServer() {
        if (IPCThreadState::freeze(mServer, true, kFreezeTimeoutMs) != OK) {
            return false;
        }

        mCgroup = std::string(kCgroupRoot) + "/binderOnewayQueueTest." + std::to_string(getpid());
        mkdir(mCgroup.c_str(), 0755);
        if (!writeFile(mCgroup + "/cgroup.procs", std::to_string(mServer)) ||
            !writeFile(mCgroup + "/cgroup.freeze", "1")) {
            return false;
        }

        const int64_t deadlineNs = nowNs() + kStartTimeoutNs;
        while (nowNs() < deadlineNs) {
            std::ifstream events(mCgroup + "/cgroup.events");
            for (std::string key, value; events >> key >> value;) {
                if (key == "frozen" && value == "1") {
                    return true;
                }
            }
            usleep(1000);
        }
        return false;
    }

    void thawServer() {
        EXPECT_EQ(OK, IPCThreadState::freeze(mServer, false, 0));
        EXPECT_TRUE(writeFile(mCgroup + "/cgroup.freeze", "0"));
    }

    pid_t mServer = -1;
    sp<IBinder> mService;
    // A second server, for tests that need another process.
    pid_t mOther = -1;
    // Cgroup that the server was moved to in order to freeze it.
    std::string mCgroup;
};

// Releasing a queued oneway call must not put it on the freeing thread's local work list. If it
// did, the thread could unexpectedly handle that call while making an outgoing Binder call.
TEST_F(BinderOnewayQueueTest, ReleasedCallDoesNotReenterDuringOutgoingOneway) {
    ASSERT_NO_FATAL_FAILURE(startServer(kOneLooper));
    // Let the looper go idle.
    usleep(kParkUs);

    const sp<IBinder> callback = sp<BBinder>::make();
    ASSERT_EQ(OK, sendOneway(0, Action::kFreeThenSendOneway, callback));
    ASSERT_EQ(OK, sendOneway(1));

    // Do not send more Binder work until the outgoing call has had time to return. A report
    // request could otherwise change how the released call is dispatched.
    usleep(kParkUs + kWakeBoundNs / 1000);

    Report report;
    ASSERT_NO_FATAL_FAILURE(waitForCalls(2, &report));
    ASSERT_TRUE(report.outgoingCallStarted);
    EXPECT_EQ(OK, report.outgoingCallStatus);
    ASSERT_EQ(2u, report.calls.size());
    EXPECT_EQ(0, report.calls[0].seq);
    EXPECT_EQ(1, report.calls[1].seq);
    EXPECT_FALSE(report.overlapped);
    EXPECT_FALSE(report.reenteredDuringOutgoingCall);
}

// When the thread that frees a buffer does not come back for more work, an idle thread has to
// be woken for the transaction that the free released.
TEST_F(BinderOnewayQueueTest, IdleLooperTakesNextCallWhileFreeingThreadIsBusy) {
    ASSERT_NO_FATAL_FAILURE(startServer(kTwoLoopers));
    // Let both loopers go idle.
    usleep(kParkUs);

    ASSERT_EQ(OK, sendOneway(0, Action::kFreeThenStayBusy));
    ASSERT_EQ(OK, sendOneway(1));

    // Stay quiet until the second call is overdue. Any call from here, including the one that
    // fetches the report, wakes the idle looper and would hide a wakeup that was never sent.
    usleep(kParkUs + kWakeBoundNs / 1000 + kParkUs);

    Report report;
    ASSERT_NO_FATAL_FAILURE(waitForCalls(2, &report));
    EXPECT_LT(report.calls[1].startNs - report.freedNs, kWakeBoundNs);
    EXPECT_NE(report.calls[0].tid, report.calls[1].tid);
}

// The thread that the driver wakes for a released transaction may have no room for it in its read
// buffer and not read again. Another idle thread has to be woken then.
TEST_F(BinderOnewayQueueTest, IdleLooperTakesNextCallWhenWokenThreadHasNoRoom) {
    ASSERT_NO_FATAL_FAILURE(startServer(kTwoLoopers));
    // Let both loopers go idle.
    usleep(kParkUs);

    ASSERT_EQ(OK, sendOneway(0, Action::kFreeForSmallReaderThenStayBusy));
    ASSERT_EQ(OK, sendOneway(1));

    // Stay quiet until the second call is overdue, as in the test above.
    usleep(kParkUs + kWakeBoundNs / 1000 + kParkUs);

    Report report;
    ASSERT_NO_FATAL_FAILURE(waitForCalls(2, &report));
    EXPECT_LT(report.calls[1].startNs - report.freedNs, kWakeBoundNs);
    EXPECT_NE(report.calls[0].tid, report.calls[1].tid);
}

// A looper frees the buffer of a call and reads in one ioctl. If that read stalls after the free,
// an idle looper has to take the released call without waiting for unrelated process work.
TEST_F(BinderOnewayQueueTest, ReleasedCallDoesNotWaitForDelayedReaderOrFreshWork) {
    ASSERT_NO_FATAL_FAILURE(startServer(kOneLooper));
    // Let the looper go idle.
    usleep(kParkUs);

    ASSERT_EQ(OK, sendOneway(-1, Action::kStartDelayedReader));
    // Time for the delayed reader to start waiting for work.
    usleep(2 * kParkUs);
    ASSERT_EQ(OK, sendOneway(0));
    ASSERT_EQ(OK, sendOneway(1));

    // Stay quiet until the reader has been let go and the second call is overdue. Any call from
    // here, including the one that fetches the report, wakes the idle looper and would hide a
    // call that was left behind.
    usleep(kParkUs + kFaultHoldUs + kReaderReturnUs + 2 * kParkUs);

    Report report;
    ASSERT_NO_FATAL_FAILURE(waitForCalls(2, &report));
    if (report.delayedReader == DelayedReader::kUnsupported) {
        GTEST_SKIP() << "cannot hold a thread in a page fault, userfaultfd is not available";
    }
    ASSERT_NE(0, report.resumedNs) << "the read did not fault on its buffer";
    ASSERT_EQ(2u, report.calls.size());
    EXPECT_EQ(0, report.calls[0].seq);
    EXPECT_EQ(1, report.calls[1].seq);
    EXPECT_FALSE(report.overlapped);
    EXPECT_GE(report.calls[1].startNs, report.freedNs);

    EXPECT_EQ(2, report.callsBeforeResume)
            << "the released call waited for the freeing thread without fresh process work";
    EXPECT_NE(report.calls[0].tid, report.calls[1].tid);
    EXPECT_LT(report.calls[1].startNs - report.freedNs, kWakeBoundNs);
}

// Measure whether a new transaction dispatches an older call while its freeing looper is held.
TEST_F(BinderOnewayQueueTest, MeasureReleasedCallWithNewProcessWork) {
    ASSERT_NO_FATAL_FAILURE(startServer(kOneLooper));
    Parcel data;
    Parcel reply;
    ASSERT_EQ(OK, mService->transact(CREATE_SERVICE, data, &reply));
    const sp<IBinder> newWorkService = reply.readStrongBinder();
    ASSERT_NE(nullptr, newWorkService);

    int pipeFds[2];
    ASSERT_EQ(0, pipe2(pipeFds, O_CLOEXEC));
    binder::unique_fd ready(pipeFds[0]);
    binder::unique_fd notify(pipeFds[1]);
    ASSERT_EQ(OK, sendOnewayTo(mService, -1, Action::kStartDelayedReader, notify));
    notify.reset();
    usleep(2 * kParkUs);
    ASSERT_EQ(OK, sendOneway(0));
    ASSERT_EQ(OK, sendOneway(1));
    const int64_t successorSentNs = nowNs();

    struct pollfd wait = {.fd = ready.get(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(1, TEMP_FAILURE_RETRY(poll(&wait, 1, kStartTimeoutNs / kNanosPerMilli)));
    ASSERT_TRUE(wait.revents & POLLIN) << "the delayed reader did not report a fault";
    int32_t state;
    ASSERT_EQ(static_cast<ssize_t>(sizeof(state)),
              TEMP_FAILURE_RETRY(read(ready.get(), &state, sizeof(state))));
    if (state == static_cast<int32_t>(DelayedReader::kUnsupported)) {
        GTEST_SKIP() << "cannot hold a thread in a page fault, userfaultfd is not available";
    }
    ASSERT_EQ(static_cast<int32_t>(DelayedReader::kStarted), state);

    // A different object supplies process work. Keep its handler busy so that a looper that
    // takes it first cannot immediately read the older call from the process queue.
    const int64_t triggerStartNs = nowNs();
    ASSERT_EQ(OK, sendOnewayTo(newWorkService, 2, Action::kFreeThenStayBusy));
    const int64_t triggerSentNs = nowNs();

    // Wait for the fault resolver to close the pipe after the delayed read returns. The report
    // requests must arrive later, since they can also dispatch queued work.
    ASSERT_EQ(1, TEMP_FAILURE_RETRY(poll(&wait, 1, kStartTimeoutNs / kNanosPerMilli)));
    ASSERT_TRUE(wait.revents & POLLHUP) << "the delayed reader did not return";

    Report report;
    ASSERT_NO_FATAL_FAILURE(waitForCalls(2, &report));
    ASSERT_EQ(2u, report.calls.size());
    ASSERT_EQ(0, report.calls[0].seq);
    ASSERT_EQ(1, report.calls[1].seq);
    EXPECT_FALSE(report.overlapped);
    ASSERT_LE(successorSentNs, report.freedNs) << "the successor was not queued before the free";
    ASSERT_LT(triggerSentNs, report.resumedNs) << "new work arrived after the reader resumed";

    Report newWorkReport;
    ASSERT_NO_FATAL_FAILURE(readReport(newWorkService, &newWorkReport));
    ASSERT_EQ(1u, newWorkReport.calls.size());
    ASSERT_EQ(2, newWorkReport.calls[0].seq);
    const int64_t startedNs = report.calls[1].startNs;
    ASSERT_GE(startedNs, report.freedNs);
    EXPECT_LT(startedNs, report.resumedNs)
            << "the released call waited for the delayed freeing thread";
    EXPECT_NE(report.calls[0].tid, report.calls[1].tid);
    EXPECT_LT(startedNs - report.freedNs, kWakeBoundNs);
    printf("Release to handler: %lld us, relative to new work submission: %lld us, "
           "started before reader resumed: %s, started before new work: %s, receiver: %s\n",
           static_cast<long long>((startedNs - report.freedNs) / 1000),
           static_cast<long long>((startedNs - triggerStartNs) / 1000),
           startedNs < report.resumedNs ? "yes" : "no",
           startedNs < newWorkReport.calls[0].startNs ? "yes" : "no",
           report.calls[0].tid == report.calls[1].tid ? "freeing looper" : "another looper");
}

// If a normal reader returns without taking released process work, a thread waiting through
// poll() must be notified. C Binder may instead let the small normal reader take the transaction;
// either receiver is valid, but the active freeing looper must not be the only path to progress.
TEST_F(BinderOnewayQueueTest, ReleasedCallReachesNormalOrPollingIdleReader) {
    ASSERT_NO_FATAL_FAILURE(startServer(kOneLooper));
    usleep(kParkUs);

    ASSERT_EQ(OK, sendOneway(0, Action::kFreeForSmallReaderAndPollThenStayBusy));
    ASSERT_EQ(OK, sendOneway(1));

    // Do not send more Binder work until the released call is overdue. A report request would
    // otherwise provide the process activity that this test intentionally withholds.
    usleep(kParkUs + kWakeBoundNs / 1000 + kParkUs);

    Report report;
    ASSERT_NO_FATAL_FAILURE(waitForCalls(2, &report));
    ASSERT_EQ(2u, report.calls.size());
    EXPECT_LT(report.calls[1].startNs - report.freedNs, kWakeBoundNs);
    EXPECT_NE(report.calls[0].tid, report.calls[1].tid);
}

// A server that polls its binder fd reads from the driver whenever the fd is readable. If the fd
// is readable without anything to deliver, that read blocks the server's event loop.
TEST_F(BinderOnewayQueueTest, PollingServerHasNothingToReadAfterLastCall) {
    ASSERT_NO_FATAL_FAILURE(startServer(kPolling));

    for (int32_t seq = 0; seq < kBurst; seq++) {
        ASSERT_EQ(OK, sendOneway(seq));
    }

    // The server checks its fd right after the last call of the burst. Send nothing until it
    // has done so, since a call from here would make the fd readable.
    usleep(kBusyUs);

    Report report;
    ASSERT_NO_FATAL_FAILURE(readReport(mService, &report))
            << "the server exits if its binder fd is readable after the burst";
    EXPECT_EQ(static_cast<size_t>(kBurst), report.calls.size());
}

// Oneway calls queued for one binder object must not keep other work for the process waiting, and
// they must still be delivered in order and one at a time.
TEST_F(BinderOnewayQueueTest, SyncCallIsServedWhileOnewayCallsAreQueued) {
    ASSERT_NO_FATAL_FAILURE(startServer(kOneLooper));

    std::atomic<int32_t> accepted = 0;
    std::thread flood([&] {
        const int64_t endNs = nowNs() + kFloodNs;
        for (int32_t seq = 0; nowNs() < endNs; seq++) {
            if (sendOneway(seq) == OK) {
                accepted++;
            }
            usleep(kFloodGapUs);
        }
    });

    // Let a backlog build up before making the synchronous call.
    usleep(kParkUs);
    Parcel data;
    Parcel reply;
    const int64_t startNs = nowNs();
    const status_t status = mService->transact(PING, data, &reply);
    const int64_t waitedNs = nowNs() - startNs;
    flood.join();

    ASSERT_EQ(OK, status);
    EXPECT_LT(waitedNs, kPingBoundNs);

    Report report;
    ASSERT_NO_FATAL_FAILURE(waitForCalls(static_cast<size_t>(accepted.load()), &report));
    EXPECT_FALSE(report.overlapped);
    EXPECT_TRUE(std::is_sorted(report.calls.begin(), report.calls.end(),
                               [](const Call& a, const Call& b) { return a.seq < b.seq; }));
}

// The driver may wake a reader for a released call that the freeing looper then takes itself. The
// woken reader has to wait again in the same read, as C Binder's reader does, instead of returning
// to userspace with nothing to do.
TEST_F(BinderOnewayQueueTest, ReaderWokenForTakenCallKeepsWaiting) {
    ASSERT_NO_FATAL_FAILURE(startServer(kOneLooper));
    // Let the looper go idle.
    usleep(kParkUs);

    // The counting reader cannot answer the call that fetches the report, so wait until it has
    // stopped reading.
    int32_t stopped = 0;
    ASSERT_NO_FATAL_FAILURE(
            sendAndWaitForValue(Action::kStartCountingReader, Action::kNone, &stopped));

    Report report;
    ASSERT_NO_FATAL_FAILURE(waitForCalls(2, &report));
    ASSERT_EQ(2u, report.calls.size());
    EXPECT_EQ(0, report.emptyReads) << "a woken reader returned from the driver with nothing to do";
    printf("The released call was started by %s\n",
           report.calls[0].tid == report.calls[1].tid ? "the freeing looper" : "the woken reader");
}

// A looper that is not waiting when a flush runs has to return from its next read without waiting,
// as in C Binder, and only from that read. A reader woken for a released call that another thread
// takes is briefly not waiting either, and relies on this to see a close of the fd at that time.
// That window is too short to hit from here, so this test uses a looper in userspace instead. The
// fd is nonblocking, so a read that would wait fails with EAGAIN, and poll() shows when the driver
// has handled the flush.
TEST_F(BinderOnewayQueueTest, FlushReachesLooperThatIsNotWaiting) {
    binder::unique_fd fd(open("/dev/binder", O_RDWR | O_NONBLOCK | O_CLOEXEC));
    ASSERT_TRUE(fd.ok());
    const uint32_t enter = BC_ENTER_LOOPER;
    binder_write_read write = {};
    write.write_size = sizeof(enter);
    write.write_buffer = reinterpret_cast<binder_uintptr_t>(&enter);
    ASSERT_EQ(0, ioctl(fd.get(), BINDER_WRITE_READ, &write));

    struct pollfd readable = {.fd = fd.get(), .events = POLLIN, .revents = 0};
    ASSERT_EQ(0, poll(&readable, 1, 0)) << "the looper had something to read before the flush";

    flushBinder(fd.get());
    const int64_t deadlineNs = nowNs() + kWakeBoundNs;
    while (poll(&readable, 1, 0) == 0 && nowNs() < deadlineNs) {
        usleep(1000);
    }
    ASSERT_TRUE(readable.revents & POLLIN) << "a flush did not reach a looper that was not waiting";

    // Large enough for the driver to look for work, see kSmallReadSize.
    uint8_t buffer[kSmallReadSize];
    binder_write_read read = {};
    read.read_size = sizeof(buffer);
    read.read_buffer = reinterpret_cast<binder_uintptr_t>(buffer);
    ASSERT_EQ(0, ioctl(fd.get(), BINDER_WRITE_READ, &read));
    EXPECT_EQ(sizeof(uint32_t), read.read_consumed) << "the read after the flush returned work";

    read.read_consumed = 0;
    const int result = ioctl(fd.get(), BINDER_WRITE_READ, &read);
    const int error = errno;
    EXPECT_EQ(-1, result) << "the flush also ended the read after the one it reached";
    EXPECT_EQ(EAGAIN, error);
}

// A synchronous call that arrives while a released call waits for the looper woken for it has to
// go to another idle looper. C Binder hands a new transaction to a waiting thread whatever process
// work is queued, which also lets it set that thread's priority before the wakeup.
TEST_F(BinderOnewayQueueTest, SyncCallGoesToIdleLooperWhileReleasedCallWaits) {
    ASSERT_NO_FATAL_FAILURE(startServer(kTwoLoopers));
    // Let both loopers go idle.
    usleep(kParkUs);

    // Whoever takes the released call stays busy for longer than the hold.
    bool held = false;
    ASSERT_NO_FATAL_FAILURE(releaseWithWokenLooperHeld(Action::kFreeThenStayBusy, &held));
    if (!held) {
        GTEST_SKIP() << "cannot keep a looper from running, CPU pinning or SCHED_FIFO is not "
                        "available";
    }

    Parcel data;
    Parcel reply;
    const int64_t startNs = nowNs();
    ASSERT_EQ(OK, mService->transact(PING, data, &reply));
    EXPECT_LT(nowNs() - startNs, kHeldBoundNs)
            << "the synchronous call waited for the released call or the held looper";
}

// Other process work that arrives while a released call waits for the looper woken for it has to
// wake an idle looper, which then takes the queued work in order. C Binder appends a death
// notification to the process queue behind the released call and wakes a waiting thread.
TEST_F(BinderOnewayQueueTest, DeathNotificationWakesIdleLooperWhileReleasedCallWaits) {
    ASSERT_NO_FATAL_FAILURE(startServer(kTwoLoopers));
    sp<IBinder> other;
    ASSERT_NO_FATAL_FAILURE(startProcess(kOneLooper, &mOther, &other));
    {
        Parcel data;
        Parcel reply;
        ASSERT_EQ(OK, data.writeStrongBinder(other));
        ASSERT_EQ(OK, mService->transact(LINK_TO_DEATH, data, &reply));
    }
    // Let both loopers go idle.
    usleep(kParkUs);

    bool held = false;
    ASSERT_NO_FATAL_FAILURE(releaseWithWokenLooperHeld(Action::kNone, &held));
    if (!held) {
        GTEST_SKIP() << "cannot keep a looper from running, CPU pinning or SCHED_FIFO is not "
                        "available";
    }
    const int64_t killedNs = nowNs();
    killProcess(&mOther);

    // Send nothing until the hold is over. Any call from here, including the one that fetches the
    // report, wakes the idle looper and would hide a wakeup that was never sent.
    usleep(kHoldUs + kParkUs);

    Report report;
    ASSERT_NO_FATAL_FAILURE(waitForCalls(2, &report));
    ASSERT_EQ(2u, report.calls.size());
    ASSERT_NE(0, report.holdEndNs);
    ASSERT_NE(0, report.deathNs) << "the death notification was not delivered";
    EXPECT_LT(report.deathNs - killedNs, kHeldBoundNs);
    EXPECT_LT(report.calls[1].startNs, report.holdEndNs)
            << "the released call waited for the held looper although another looper was woken";
}

// The death of a server has to be reported while oneway calls are queued for it, and the driver
// has to keep working for other processes afterwards.
TEST_F(BinderOnewayQueueTest, ServerDeathWithCallsQueuedIsReported) {
    ASSERT_NO_FATAL_FAILURE(startServer(kOneLooper));
    // Death notifications are delivered to the thread pool.
    ProcessState::self()->startThreadPool();
    const sp<DeathFlag> recipient = sp<DeathFlag>::make();
    ASSERT_EQ(OK, mService->linkToDeath(recipient));

    for (int32_t seq = 0; seq < kBacklog; seq++) {
        ASSERT_EQ(OK, sendOneway(seq));
    }
    killServer();

    const int64_t deadlineNs = nowNs() + kDeathBoundNs;
    while (!recipient->died && nowNs() < deadlineNs) {
        usleep(1000);
    }
    EXPECT_TRUE(recipient->died);
    EXPECT_EQ(DEAD_OBJECT, sendOneway(kBacklog));

    ASSERT_NO_FATAL_FAILURE(startServer(kOneLooper));
    ASSERT_EQ(OK, sendOneway(0));
    Report report;
    ASSERT_NO_FATAL_FAILURE(waitForCalls(1, &report));
}

// A server with two loopers is killed again and again while a sender floods one of its binder
// objects without pause, so that calls are queued for the object, on the way to its threads and
// rejected for lack of space when it dies. Each time the sender has to see the death, and a new
// server has to be served. This looks for transactions or threads that the driver mishandles
// while it tears a process down.
TEST_F(BinderOnewayQueueTest, ServerKilledRepeatedlyDuringFlood) {
    for (int round = 0; round < kKillRounds; round++) {
        ASSERT_NO_FATAL_FAILURE(startServer(kTwoLoopers));
        const sp<IBinder> service = mService;

        std::atomic<bool> stop = false;
        std::atomic<bool> sawDeath = false;
        std::thread flood([&] {
            int32_t seq = 0;
            while (!stop) {
                Parcel data;
                data.writeInt32(seq);
                data.writeInt32(static_cast<int32_t>(Action::kNone));
                const status_t status =
                        service->transact(ONEWAY, data, nullptr, IBinder::FLAG_ONEWAY);
                if (status == OK) {
                    seq++;
                } else if (status == DEAD_OBJECT) {
                    sawDeath = true;
                    usleep(1000);
                }
                // Any other status is a call rejected by a full buffer. Keep flooding.
            }
        });

        usleep(kFloodBeforeKillUs);
        killServer();
        const int64_t deadlineNs = nowNs() + kDeathBoundNs;
        while (!sawDeath && nowNs() < deadlineNs) {
            usleep(1000);
        }
        stop = true;
        flood.join();
        ASSERT_TRUE(sawDeath) << "round " << round << ": the sender did not see the death";
    }

    ASSERT_NO_FATAL_FAILURE(startServer(kOneLooper));
    ASSERT_EQ(OK, sendOneway(0));
    Report report;
    ASSERT_NO_FATAL_FAILURE(waitForCalls(1, &report));
}

// Oneway calls sent to a frozen server wait in the driver. Once the server is thawed they have to
// be delivered in order and one at a time, like any other backlog.
TEST_F(BinderOnewayQueueTest, CallsSentToFrozenServerAreDeliveredAfterThaw) {
    ASSERT_NO_FATAL_FAILURE(startServer(kOneLooper));
    // Let the looper go idle.
    usleep(kParkUs);
    if (!freezeServer()) {
        GTEST_SKIP() << "the server could not be frozen";
    }

    for (int32_t seq = 0; seq < kBacklog; seq++) {
        ASSERT_EQ(OK, sendOneway(seq));
    }
    // Long enough for a server that is not frozen to start on the calls.
    usleep(kParkUs);
    const int64_t thawedNs = nowNs();
    thawServer();

    Report report;
    ASSERT_NO_FATAL_FAILURE(waitForCalls(static_cast<size_t>(kBacklog), &report));
    EXPECT_GE(report.calls.front().startNs, thawedNs);
    EXPECT_FALSE(report.overlapped);
    EXPECT_TRUE(std::is_sorted(report.calls.begin(), report.calls.end(),
                               [](const Call& a, const Call& b) { return a.seq < b.seq; }));
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc == 4 && strcmp(argv[1], kServerFlag) == 0) {
        return serve(argv[2], String16(argv[3]));
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
