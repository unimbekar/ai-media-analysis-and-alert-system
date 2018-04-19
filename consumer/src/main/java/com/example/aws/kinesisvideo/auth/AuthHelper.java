package com.example.aws.kinesisvideo.auth;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

public final class AuthHelper {
    public static AWSCredentialsProvider getSystemPropertiesCredentialsProvider() {
        return new SystemPropertiesCredentialsProvider();
    }

    public static AWSCredentialsProvider getDefaultPropertiesCredentialsProvider() {
        return new ProfileCredentialsProvider();
    }

    public static AWSCredentialsProvider getInstanceProfieCredentialsProvider() {
        return new InstanceProfileCredentialsProvider();
    }

    private AuthHelper() {
        throw new UnsupportedOperationException();
    }
}
