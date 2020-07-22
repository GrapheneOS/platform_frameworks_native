/*
 * Copyright (C) 2019 The Android Open Source Project
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

#ifndef _UI_INPUT_INPUTDISPATCHER_INPUTDISPATCHERINTERFACE_H
#define _UI_INPUT_INPUTDISPATCHER_INPUTDISPATCHERINTERFACE_H

#include <InputListener.h>
#include <android/os/ISetInputWindowsListener.h>
#include <input/InputApplication.h>
#include <input/InputTransport.h>
#include <input/InputWindow.h>
#include <unordered_map>

namespace android {

/*
 * Constants used to report the outcome of input event injection.
 */
enum {
    /* (INTERNAL USE ONLY) Specifies that injection is pending and its outcome is unknown. */
    INPUT_EVENT_INJECTION_PENDING = -1,

    /* Injection succeeded. */
    INPUT_EVENT_INJECTION_SUCCEEDED = 0,

    /* Injection failed because the injector did not have permission to inject
     * into the application with input focus. */
    INPUT_EVENT_INJECTION_PERMISSION_DENIED = 1,

    /* Injection failed because there were no available input targets. */
    INPUT_EVENT_INJECTION_FAILED = 2,

    /* Injection failed due to a timeout. */
    INPUT_EVENT_INJECTION_TIMED_OUT = 3
};

/* Notifies the system about input events generated by the input reader.
 * The dispatcher is expected to be mostly asynchronous. */
class InputDispatcherInterface : public virtual RefBase, public InputListenerInterface {
protected:
    InputDispatcherInterface() {}
    virtual ~InputDispatcherInterface() {}

public:
    /* Dumps the state of the input dispatcher.
     *
     * This method may be called on any thread (usually by the input manager). */
    virtual void dump(std::string& dump) = 0;

    /* Called by the heatbeat to ensures that the dispatcher has not deadlocked. */
    virtual void monitor() = 0;

    /**
     * Wait until dispatcher is idle. That means, there are no further events to be processed,
     * and all of the policy callbacks have been completed.
     * Return true if the dispatcher is idle.
     * Return false if the timeout waiting for the dispatcher to become idle has expired.
     */
    virtual bool waitForIdle() = 0;

    /* Make the dispatcher start processing events.
     *
     * The dispatcher will start consuming events from the InputListenerInterface
     * in the order that they were received.
     */
    virtual status_t start() = 0;

    /* Makes the dispatcher stop processing events. */
    virtual status_t stop() = 0;

    /* Injects an input event and optionally waits for sync.
     * The synchronization mode determines whether the method blocks while waiting for
     * input injection to proceed.
     * Returns one of the INPUT_EVENT_INJECTION_XXX constants.
     *
     * This method may be called on any thread (usually by the input manager).
     */
    virtual int32_t injectInputEvent(const InputEvent* event, int32_t injectorPid,
                                     int32_t injectorUid, int32_t syncMode,
                                     std::chrono::milliseconds timeout, uint32_t policyFlags) = 0;

    /*
     * Check whether InputEvent actually happened by checking the signature of the event.
     *
     * Return nullptr if the event cannot be verified.
     */
    virtual std::unique_ptr<VerifiedInputEvent> verifyInputEvent(const InputEvent& event) = 0;

    /* Sets the list of input windows per display.
     *
     * This method may be called on any thread (usually by the input manager).
     */
    virtual void setInputWindows(
            const std::unordered_map<int32_t, std::vector<sp<InputWindowHandle>>>&
                    handlesPerDisplay) = 0;

    /* Sets the focused application on the given display.
     *
     * This method may be called on any thread (usually by the input manager).
     */
    virtual void setFocusedApplication(
            int32_t displayId, const sp<InputApplicationHandle>& inputApplicationHandle) = 0;

    /* Sets the focused display.
     *
     * This method may be called on any thread (usually by the input manager).
     */
    virtual void setFocusedDisplay(int32_t displayId) = 0;

    /* Sets the input dispatching mode.
     *
     * This method may be called on any thread (usually by the input manager).
     */
    virtual void setInputDispatchMode(bool enabled, bool frozen) = 0;

    /* Sets whether input event filtering is enabled.
     * When enabled, incoming input events are sent to the policy's filterInputEvent
     * method instead of being dispatched.  The filter is expected to use
     * injectInputEvent to inject the events it would like to have dispatched.
     * It should include POLICY_FLAG_FILTERED in the policy flags during injection.
     */
    virtual void setInputFilterEnabled(bool enabled) = 0;

    /**
     * Set the touch mode state.
     * Touch mode is a global state that apps may enter / exit based on specific
     * user interactions with input devices.
     * If true, the device is in touch mode.
     */
    virtual void setInTouchMode(bool inTouchMode) = 0;

    /* Transfers touch focus from one window to another window.
     *
     * Returns true on success.  False if the window did not actually have touch focus.
     */
    virtual bool transferTouchFocus(const sp<IBinder>& fromToken, const sp<IBinder>& toToken) = 0;

    /* Registers input channels that may be used as targets for input events.
     *
     * This method may be called on any thread (usually by the input manager).
     */
    virtual status_t registerInputChannel(const std::shared_ptr<InputChannel>& inputChannel) = 0;

    /* Registers input channels to be used to monitor input events.
     *
     * Each monitor must target a specific display and will only receive input events sent to that
     * display. If the monitor is a gesture monitor, it will only receive pointer events on the
     * targeted display.
     *
     * This method may be called on any thread (usually by the input manager).
     */
    virtual status_t registerInputMonitor(const std::shared_ptr<InputChannel>& inputChannel,
                                          int32_t displayId, bool gestureMonitor) = 0;

    /* Unregister input channels that will no longer receive input events.
     *
     * This method may be called on any thread (usually by the input manager).
     */
    virtual status_t unregisterInputChannel(const InputChannel& inputChannel) = 0;

    /* Allows an input monitor steal the current pointer stream away from normal input windows.
     *
     * This method may be called on any thread (usually by the input manager).
     */
    virtual status_t pilferPointers(const sp<IBinder>& token) = 0;
};

} // namespace android

#endif // _UI_INPUT_INPUTDISPATCHER_INPUTDISPATCHERINTERFACE_H
