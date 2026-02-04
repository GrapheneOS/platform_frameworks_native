package android.content.pm;

/**
 * @hide
 */
parcelable MicrophoneScopeInfo {
    /** Whether microphone scopes are enabled for this app */
    boolean enabled;

    /** Path to the audio file in /data/system/microphone_scopes/ */
    @nullable @utf8InCpp String audioFilePath;
}
