/*
 * Copyright (C) 2026 GrapheneOS
 * SPDX-License-Identifier: Apache-2.0
 */

package android.os;

/** @hide */
interface IMediaDrmIdAccessService {
    const String SERVICE_NAME = "media_drm_id_access";

    /**
     * Checks access for the app which made the current request to a DRM HAL.
     *
     * The caller is a trusted DRM HAL. uid and pid are captured by libbinder_ndk from the incoming
     * Binder transaction rather than supplied by the app.
     */
    boolean isAllowedFromDrmHal(int uid, int pid);

}
