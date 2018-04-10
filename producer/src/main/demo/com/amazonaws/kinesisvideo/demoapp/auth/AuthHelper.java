package com.amazonaws.kinesisvideo.demoapp.auth;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

public final class AuthHelper {
    public static AWSCredentialsProvider getSystemPropertiesCredentialsProvider() {
        return new SystemPropertiesCredentialsProvider();
    }

    public static AWSCredentialsProvider getDefaultPropertiesCredentialsProvider() {
        return new ProfileCredentialsProvider();
    }
    private AuthHelper() {
        throw new UnsupportedOperationException();
    }
}
