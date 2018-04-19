package com.amazonaws.kinesisvideo.demoapp;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.kinesisvideo.demoapp.auth.AuthHelper;
import com.amazonaws.kinesisvideo.demoapp.common.Constants;
import com.amazonaws.kinesisvideo.demoapp.utils.S3Utils;
import com.amazonaws.services.kinesisvideo.*;
import com.amazonaws.services.kinesisvideo.model.AckEvent;
import com.amazonaws.services.kinesisvideo.model.FragmentTimecodeType;
import com.amazonaws.services.kinesisvideo.model.GetDataEndpointRequest;
import com.amazonaws.services.kinesisvideo.model.PutMediaRequest;
import org.apache.commons.lang3.StringUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.CountDownLatch;


/**
 * An example on how to send an MKV file to Kinesis Video Streams.
 *
 * If you have other video formats, you can use ffmpeg to convert to MKV. Only H264 videos are playable in the console.
 * Steps to convert MP4 to MKV:
 *
 * 1. Install ffmpeg if not yet done so already:
 *
 *      Mac OS X:
 *          brew install ffmpeg --with-opus --with-fdk-aac --with-tools --with-freetype --with-libass --with-libvorbis
 *          --with-libvpx --with-x265 --with-libopus
 *
 *      Others:
 *          git clone https://git.ffmpeg.org/ffmpeg.git ffmpeg
 *          ./configure
 *          make
 *          make install
 *
 *  2. Convert MP4 to MKV
 *      ffmpeg -i input.mp4 -b:v 10M -minrate 10M -maxrate 10M -bufsize 10M -bf 0 input.mkv
 */
public final class KinesisVideoProducer {
    private static final String DEFAULT_REGION = "us-east-1";
    private static final String PUT_MEDIA_API = "/putMedia";

    /* the name of the stream */
    private static final String STREAM_NAME = System.getProperty("stream.name", "play-mkv-stream");

    /* sample MKV file */
    private static final String DEFAULT_MKV_FILE_PATH = System.getProperty("movie.path", "producer/src/main/resources/data/mkv/clusters.mkv");
//    big_buck_bunny_480p_h264.mkv does NOT work. errorCode=MORE_THAN_ONE_TRACK_FOUND
//    private static final String DEFAULT_MKV_FILE_PATH = "src/main/resources/data/mkv/big_buck_bunny_480p_h264.mkv";

    // Specify source as s3.bucket. If Specified, media is downloaded from S3 using the input s3.key param.
    private static final String S3_BUCKET = System.getProperty("s3.bucket", null);

    //S3 Key Param: full S3 Key of the Movie file to be downloaded
    private static final String S3_KEY = System.getProperty("s3.key", null);

    private static final String S3_DOWNLOAD_DIR = System.getProperty("s3.download.dir", Constants.S3_DOWNLOAD_DIR);

    /* max upload bandwidth */
    private static final long MAX_BANDWIDTH_KBPS = 15 * 1024L;

    /* response read timeout */
    private static final int READ_TIMEOUT_IN_MILLIS = 1_000_000;

    /* connect timeout */
    private static final int CONNECTION_TIMEOUT_IN_MILLIS = 10_000;

    private static String mkvFilePath = DEFAULT_MKV_FILE_PATH;

    private KinesisVideoProducer() { }

    private static void printParams() {
        System.out.println("S3_BUCKET: " + S3_BUCKET);
        System.out.println("S3_KEY: " + S3_KEY);
        System.out.println("STREAM_NAME: " + STREAM_NAME);
        System.out.println("S3_DOWNLOAD_DIR: " + S3_DOWNLOAD_DIR);
        System.out.println("MKV_FILE_PATH: " + mkvFilePath);
    }

    public static void main(final String[] args) throws Exception {
        AWSCredentialsProvider awsCredentials = new DefaultAWSCredentialsProviderChain();

        final AmazonKinesisVideo frontendClient = AmazonKinesisVideoClientBuilder.standard()
                .withCredentials(awsCredentials)
                .withRegion(DEFAULT_REGION)
                .build();

        /* this is the endpoint returned by GetDataEndpoint API */
        final String dataEndpoint = frontendClient.getDataEndpoint(
                new GetDataEndpointRequest()
                        .withStreamName(STREAM_NAME)
                        .withAPIName("PUT_MEDIA")).getDataEndpoint();

        //Check if s3.bucket and s3.key are specified
        if(!StringUtils.isEmpty(S3_BUCKET) &&  !StringUtils.isEmpty(S3_KEY)) {
            S3Utils s3Utils = new S3Utils(S3_BUCKET);

            String downloadFilePath = S3_DOWNLOAD_DIR +
                    "/" + S3_BUCKET +
                    "/" + S3_KEY;

            s3Utils.downloadFromS3(S3_KEY, downloadFilePath);

            System.out.println(downloadFilePath + " file has been download successfully from Bucket: " + S3_BUCKET);

            if(Files.exists(Paths.get(downloadFilePath))) {
                mkvFilePath = downloadFilePath;
            }
        }

        printParams();

        /* send the same MKV file over and over */
        while (true) {
            /* actually URI to send PutMedia request */
            final URI uri = URI.create(dataEndpoint + PUT_MEDIA_API);

            /* input stream for sample MKV file */
            final InputStream inputStream = new FileInputStream(mkvFilePath);

            /* use a latch for main thread to wait for response to complete */
            final CountDownLatch latch = new CountDownLatch(1);

            /* PutMedia client */
            final AmazonKinesisVideoPutMedia dataClient = AmazonKinesisVideoPutMediaClient.builder()
                    .withRegion(DEFAULT_REGION)
                    .withEndpoint(URI.create(dataEndpoint))
                    .withCredentials(AuthHelper.getDefaultPropertiesCredentialsProvider())
                    .withConnectionTimeoutInMillis(CONNECTION_TIMEOUT_IN_MILLIS)
                    .build();

            final PutMediaAckResponseHandler responseHandler = new PutMediaAckResponseHandler()  {
                @Override
                public void onAckEvent(AckEvent event) {
                    System.out.println("onAckEvent " + event);
                }

                @Override
                public void onFailure(Throwable t) {
                    latch.countDown();
                    throw new RuntimeException(t);
                }

                @Override
                public void onComplete() {
                    System.out.println("onComplete");
                    latch.countDown();
                }
            };

            /* start streaming video in a background thread */
            dataClient.putMedia(new PutMediaRequest()
                            .withStreamName(STREAM_NAME)
                            .withFragmentTimecodeType(FragmentTimecodeType.RELATIVE)
                            .withPayload(inputStream)
                            .withProducerStartTimestamp(Date.from(Instant.now())),
                    responseHandler);

            /* wait for request/response to complete */
            latch.await();

            /* close the client */
            dataClient.close();
        }
    }
}
