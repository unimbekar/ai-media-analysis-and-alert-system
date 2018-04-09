package com.amazonaws.kinesisvideo.service;

import com.amazonaws.kinesisvideo.common.exception.KinesisVideoException;
import com.amazonaws.kinesisvideo.common.function.Consumer;
import com.amazonaws.kinesisvideo.encoding.ChunkDecoder;
import com.amazonaws.kinesisvideo.model.ResponseStatus;
import com.amazonaws.kinesisvideo.service.exception.AccessDeniedException;
import com.amazonaws.kinesisvideo.service.exception.AmazonServiceException;
import com.amazonaws.kinesisvideo.service.exception.ResourceNotFoundException;

import javax.annotation.Nonnull;

import static com.amazonaws.kinesisvideo.common.preconditions.Preconditions.checkNotNull;

import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class BlockingAckConsumer implements Consumer<InputStream> {
    private static final long RESPONSE_TIMEOUT_IN_MILLISECONDS = 10000;
    private static final int HTTP_OK = 200;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_ACCESS_DENIED = 403;
    private final Consumer<InputStream> inputStreamConsumer;
    private final CountDownLatch responseLatch;
    private Exception storedException;

    public BlockingAckConsumer(@Nonnull final Consumer<InputStream> inputStreamConsumer) {
        this.inputStreamConsumer = checkNotNull(inputStreamConsumer);
        this.responseLatch = new CountDownLatch(1);
    }

    @Override
    public void accept(final @Nonnull InputStream inputStream) {
        checkNotNull(inputStream);

        // Await for the header
        try {
            final ResponseStatus responseStatus = ChunkDecoder.readStatusLine(inputStream);
            final int responseCode = responseStatus.getStatusCode();
            switch (responseCode) {
                case HTTP_OK:
                    break;
                case HTTP_BAD_REQUEST:
                    throw new AmazonServiceException("PutMedia call returned bad request status code");
                case HTTP_NOT_FOUND:
                    throw new ResourceNotFoundException("Resource not found");
                case HTTP_ACCESS_DENIED:
                    throw new AccessDeniedException("Access is denied");
                default:
                    throw new AmazonServiceException("PutMedia call returned status code " + responseCode);
            }
        } catch (final Exception e) {
            // Store the exception
            storedException = e;
        }
        finally {
            responseLatch.countDown();
        }

        // Forward to the origin if no exceptions have been thrown
        if (storedException == null) {
            inputStreamConsumer.accept(inputStream);
        }
    }

    public void awaitResponse() throws KinesisVideoException {
        // Block until loop finished of timed out.
        try {
            if (!responseLatch.await(RESPONSE_TIMEOUT_IN_MILLISECONDS, TimeUnit.MILLISECONDS)) {
                throw new KinesisVideoException("Getting PutMedia Response timed out");
            }
        } catch (final InterruptedException e) {
            throw new KinesisVideoException(e);
        } finally {
            if (storedException != null) {
                if (storedException instanceof KinesisVideoException) {
                    throw (KinesisVideoException) storedException;
                } else {
                    throw new KinesisVideoException(storedException);
                }
            }
        }
    }
}