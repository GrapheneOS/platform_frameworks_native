/*
 * Copyright (C) 2018 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <android/binder_ibinder.h>
#include <android/binder_shell.h>
#include "ibinder_internal.h"

#include <atomic>
#include <cstring>
#include <mutex>
#include <optional>
#include <vector>

#include <binder/Binder.h>
#include <binder/IBinder.h>
#include <binder/Parcel.h>
#include <utils/Vector.h>

inline bool isUserCommand(transaction_code_t code) {
    return code >= FIRST_CALL_TRANSACTION && code <= LAST_CALL_TRANSACTION;
}

struct ABBinder;
struct ABpBinder;

struct AIBinder : public virtual ::android::RefBase {
    explicit AIBinder(const AIBinder_Class* clazz);
    virtual ~AIBinder();

    bool associateClass(const AIBinder_Class* clazz);
    const AIBinder_Class* getClass() const { return mClazz; }

    virtual ::android::sp<::android::IBinder> getBinder() = 0;
    virtual ABBinder* asABBinder() { return nullptr; }
    virtual ABpBinder* asABpBinder() { return nullptr; }

    bool isRemote() const {
        ::android::sp<::android::IBinder> binder = const_cast<AIBinder*>(this)->getBinder();
        return binder->remoteBinder() != nullptr;
    }
    virtual void addDeathRecipient(const ::android::sp<AIBinder_DeathRecipient>& recipient,
                                   void* cookie) = 0;

    virtual void addFrozenStateChangeCallback(
            const ::android::sp<AIBinder_FrozenStateChangeCallback>& recipient, void* cookie) = 0;

   private:
    // AIBinder instance is instance of this class for a local object. In order to transact on a
    // remote object, this also must be set for simplicity (although right now, only the
    // interfaceDescriptor from it is used).
    //
    // WARNING: When multiple classes exist with the same interface descriptor in different
    // linkernamespaces, the first one to be associated with mClazz becomes the canonical one
    // and the only requirement on this is that the interface descriptors match. If this
    // is an ABpBinder, no other state can be referenced from mClazz.
    const AIBinder_Class* mClazz;
    std::mutex mClazzMutex;
};

// This is a local AIBinder object with a known class.
struct ABBinder : public AIBinder, public ::android::BBinder {
    virtual ~ABBinder();

    void* getUserData() { return mUserData; }

    ::android::sp<::android::IBinder> getBinder() override { return this; }
    ABBinder* asABBinder() override { return this; }

    const ::android::String16& getInterfaceDescriptor() const override;
    ::android::status_t dump(int fd, const ::android::Vector<::android::String16>& args) override;
    ::android::status_t onTransact(uint32_t code, const ::android::Parcel& data,
                                   ::android::Parcel* reply, binder_flags_t flags) override;
    void addDeathRecipient(const ::android::sp<AIBinder_DeathRecipient>& /* recipient */,
                           void* /* cookie */) override;
    void addFrozenStateChangeCallback(
            const ::android::sp<AIBinder_FrozenStateChangeCallback>& /* recipient */,
            void* /* cookie */) override;

   private:
    ABBinder(const AIBinder_Class* clazz, void* userData);

    friend android::sp<ABBinder>;
    // only thing that should create an ABBinder
    friend AIBinder* AIBinder_new(const AIBinder_Class*, void*);

    // Can contain implementation if this is a local binder. This can still be nullptr for a local
    // binder. If it is nullptr, the implication is the implementation state is entirely external to
    // this object and the functionality provided in the AIBinder_Class is sufficient.
    void* mUserData;
};

// This binder object may be remote or local (even though it is 'Bp'). The implication if it is
// local is that it is an IBinder object created outside of the domain of libbinder_ndk.
struct ABpBinder : public AIBinder {
    // Looks up to see if this object has or is an existing ABBinder or ABpBinder object, otherwise
    // it creates an ABpBinder object.
    static ::android::sp<AIBinder> lookupOrCreateFromBinder(
            const ::android::sp<::android::IBinder>& binder);

    virtual ~ABpBinder();

    ::android::sp<::android::IBinder> getBinder() override { return mRemote; }
    ABpBinder* asABpBinder() override { return this; }

    bool isServiceFuzzing() const { return mServiceFuzzing; }
    void setServiceFuzzing() { mServiceFuzzing = true; }
    void addDeathRecipient(const ::android::sp<AIBinder_DeathRecipient>& recipient,
                           void* cookie) override;
    void addFrozenStateChangeCallback(
            const ::android::sp<AIBinder_FrozenStateChangeCallback>& recipient,
            void* cookie) override;

   private:
    friend android::sp<ABpBinder>;
    explicit ABpBinder(const ::android::sp<::android::IBinder>& binder);
    ::android::sp<::android::IBinder> mRemote;
    bool mServiceFuzzing = false;
    struct DeathRecipientInfo {
        android::wp<AIBinder_DeathRecipient> recipient;
        void* cookie;
    };
    std::mutex mDeathRecipientsMutex;
    std::vector<DeathRecipientInfo> mDeathRecipients;

    struct FrozenStateChangeCallbackInfo {
        android::wp<AIBinder_FrozenStateChangeCallback> recipient;
        void* cookie;
    };
    std::mutex mFrozenStateChangeCallbacksMutex;
    std::vector<FrozenStateChangeCallbackInfo> mFrozenStateChangeCallbacks;
};

struct AIBinder_Class {
    AIBinder_Class(const char* interfaceDescriptor, AIBinder_Class_onCreate onCreate,
                   AIBinder_Class_onDestroy onDestroy, AIBinder_Class_onTransact onTransact);

    const ::android::String16& getInterfaceDescriptor() const { return mWideInterfaceDescriptor; }
    const char* getInterfaceDescriptorUtf8() const { return mInterfaceDescriptor.c_str(); }
    bool isMediaDrmPlugin() const { return mIsMediaDrmPlugin; }
    bool setTransactionCodeMap(const char* const* transactionCodeMap,
                               size_t transactionCodeMapSize);
    const char* getFunctionName(transaction_code_t code) const;
    size_t getTransactionCodeToFunctionLength() const { return mTransactionCodeData.count; }

    // whether a transaction header should be written
    bool writeHeader = true;

    // required to be non-null, implemented for every class
    const AIBinder_Class_onCreate onCreate = nullptr;
    const AIBinder_Class_onDestroy onDestroy = nullptr;
    const AIBinder_Class_onTransact onTransact = nullptr;

    // optional methods for a class
    AIBinder_onDump onDump = nullptr;
    AIBinder_handleShellCommand handleShellCommand = nullptr;

   private:
    // Copy of the raw char string for when we don't have to return UTF-16
    const std::string mInterfaceDescriptor;
    const bool mIsMediaDrmPlugin;
    // This must be a String16 since BBinder virtual getInterfaceDescriptor returns a reference to
    // one.
    const ::android::String16 mWideInterfaceDescriptor;

   public:
    // struct which holds names of the functions and count
    alignas(16) android::TransactionCodeData mTransactionCodeData;
};

// Identifies the stable AIDL operation protected by the MediaDrm ID policy. The Parcel is
// positioned after the interface token, as it is in ABBinder::onTransact, and is always restored.
// DRM HALs generated for API levels before transaction-name metadata was introduced are handled
// conservatively by matching the first string argument without relying on a raw transaction
// number.
inline ::android::status_t AIBinder_isMediaDrmDeviceUniqueIdRequest(const AIBinder_Class* clazz,
                                                                    transaction_code_t code,
                                                                    const ::android::Parcel& data,
                                                                    bool* isRequest) {
    *isRequest = false;
    if (!clazz->isMediaDrmPlugin() || code < FIRST_CALL_TRANSACTION) {
        return ::android::OK;
    }
    const bool hasFunctionNames = clazz->mTransactionCodeData.names != nullptr;
    if (hasFunctionNames) {
        const char* const functionName = AIBinder_Class_getFunctionName(clazz, code);
        if (functionName == nullptr || strcmp(functionName, "getPropertyByteArray") != 0) {
            return ::android::OK;
        }
    }

    const size_t position = data.dataPosition();
    std::string propertyName;
    const ::android::status_t status = data.readUtf8FromUtf16(&propertyName);
    data.setDataPosition(position);
    if (status != ::android::OK) {
        return hasFunctionNames ? status : ::android::OK;
    }
    *isRequest = propertyName == "deviceUniqueId";
    return ::android::OK;
}

template <typename Base, typename Parent, typename Callback, typename UnlinkedCallback>
struct TransferRecipient : Base {
    TransferRecipient(const ::android::wp<::android::IBinder>& who, void* cookie,
                      const ::android::wp<Parent>& parentRecipient, const Callback onCallback,
                      const UnlinkedCallback onUnlinked)
        : mWho(who),
          mCookie(cookie),
          mParentRecipient(parentRecipient),
          mOnCallback(onCallback),
          mOnUnlinked(onUnlinked) {}

    virtual ~TransferRecipient() {
        if (mOnUnlinked != nullptr) {
            mOnUnlinked(mCookie);
        }
    }

    const ::android::wp<::android::IBinder>& getWho() { return mWho; }
    void* getCookie() { return mCookie; }

   protected:
    ::android::wp<::android::IBinder> mWho;
    void* mCookie;
    ::android::wp<Parent> mParentRecipient;
    const Callback mOnCallback;
    const UnlinkedCallback mOnUnlinked;
};

template <typename Wrapper>
struct RecipientList {
    std::mutex mMutex;
    std::vector<::android::sp<Wrapper>> mList;

    void pruneThisTransferEntry(const ::android::sp<::android::IBinder>& who, void* cookie) {
        std::lock_guard<std::mutex> l(mMutex);
        mList.erase(std::remove_if(mList.begin(), mList.end(),
                                   [&](const ::android::sp<Wrapper>& tdr) {
                                       auto tdrWho = tdr->getWho();
                                       return tdrWho != nullptr && tdrWho.promote() == who &&
                                              cookie == tdr->getCookie();
                                   }),
                    mList.end());
    }

    void pruneDeadTransferEntriesLocked() {
        mList.erase(std::remove_if(mList.begin(), mList.end(),
                                   [](const ::android::sp<Wrapper>& tdr) {
                                       return tdr->getWho() == nullptr;
                                   }),
                    mList.end());
    }
};

// Ownership is like this (when linked to death):
//
//   AIBinder_DeathRecipient -sp-> TransferDeathRecipient <-wp-> IBinder
//
// When the AIBinder_DeathRecipient is dropped, so are the actual underlying death recipients. When
// the IBinder dies, only a wp to it is kept.
struct AIBinder_DeathRecipient : ::android::RefBase {
    // One of these is created for every linkToDeath. This is to be able to recover data when a
    // binderDied receipt only gives us information about the IBinder.
    struct TransferDeathRecipient
        : TransferRecipient<::android::IBinder::DeathRecipient, AIBinder_DeathRecipient,
                            AIBinder_DeathRecipient_onBinderDied,
                            AIBinder_DeathRecipient_onBinderUnlinked> {
        using TransferRecipient::TransferRecipient;
        void binderDied(const ::android::wp<::android::IBinder>& who) override;
    };

    explicit AIBinder_DeathRecipient(AIBinder_DeathRecipient_onBinderDied onDied);
    binder_status_t linkToDeath(const ::android::sp<::android::IBinder>&, void* cookie);
    binder_status_t unlinkToDeath(const ::android::sp<::android::IBinder>& binder, void* cookie);
    void setOnUnlinked(AIBinder_DeathRecipient_onBinderUnlinked onUnlinked);
    void pruneThisTransferEntry(const ::android::sp<::android::IBinder>&, void* cookie);

   private:
    friend struct TransferDeathRecipient;
    RecipientList<TransferDeathRecipient> mDeathRecipients;
    AIBinder_DeathRecipient_onBinderDied mOnDied;
    AIBinder_DeathRecipient_onBinderUnlinked mOnUnlinked;
};

// Ownership is like this (when added):
//
//   AIBinder_FrozenStateChangeCallback -sp-> TransferFrozenStateChangeCallback <-wp-> IBinder
//
// When the AIBinder_FrozenStateChangeCallback is dropped, so are the actual underlying
// callbacks. When the IBinder dies, only a wp to it is kept.
struct AIBinder_FrozenStateChangeCallback : ::android::RefBase {
    // One of these is created for every addFrozenStateChangeCallback. This is to be able to recover
    // data when a onStateChanged receipt only gives us information about the IBinder.
    struct TransferFrozenStateChangeCallback
        : TransferRecipient<::android::IBinder::FrozenStateChangeCallback,
                            AIBinder_FrozenStateChangeCallback,
                            AIBinder_FrozenStateChangeCallback_onStateChanged,
                            AIBinder_FrozenStateChangeCallback_onBinderUnlinked> {
        using TransferRecipient::TransferRecipient;
        void onStateChanged(const ::android::wp<::android::IBinder>& who, State state) override;
    };

    AIBinder_FrozenStateChangeCallback(
            AIBinder_FrozenStateChangeCallback_onStateChanged onStateChanged,
            AIBinder_FrozenStateChangeCallback_onBinderUnlinked onUnlinked);
    [[nodiscard]] binder_status_t addFrozenStateChangeCallback(
            const ::android::sp<::android::IBinder>&, void* cookie);
    [[nodiscard]] binder_status_t removeFrozenStateChangeCallback(
            const ::android::sp<::android::IBinder>& binder, void* cookie);
    void pruneThisTransferEntry(const ::android::sp<::android::IBinder>&, void* cookie);

   private:
    friend struct TransferFrozenStateChangeCallback;
    RecipientList<TransferFrozenStateChangeCallback> mFrozenStateChangeCallbacks;
    AIBinder_FrozenStateChangeCallback_onStateChanged mOnStateChanged;
    AIBinder_FrozenStateChangeCallback_onBinderUnlinked mOnUnlinked;
};
