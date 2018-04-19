package com.amazonaws.kinesisvideo.demoapp.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.kinesisvideo.demoapp.common.Constants;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * This sample demonstrates how to make basic requests to Amazon S3 using the
 * AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon S3. For more information on Amazon
 * S3, see http://aws.amazon.com/s3.
 * <p>
 * Fill in your AWS access credentials in the provided credentials file
 * template, and be sure to move the file to the default location
 * (~/.aws/credentials) where the sample code will load the credentials from.
 * <p>
 * <b>WARNING:</b> To avoid accidental leakage of your credentials, DO NOT keep
 * the credentials file in your source directory.
 * <p>
 * http://aws.amazon.com/security-credentials
 */
public class S3Utils {

    private static AWSCredentials credentials = null;
    private static AmazonS3 s3 = null;
    private static final String LOG_TAG = "S3Store";
    private static String bucketName = Constants.S3_BUCKET_NAME;
    private String bucket = null;
    private String key = null;
    private static String SAGE_BUCKET = "sage-media-bucket";
    private static String CLUSTER_MKV_KEY = "demo/mkv/clusters.mkv";
    private static String JFISH_MKV_KEY = "demo/mkv/jfish.mkv";
    private static String MKV_DOWNLOAD_PATH = "/tmp/s3/downloads/clusters.mkv";

    static {
        try {
            /*
             * The ProfileCredentialsProvider will return your [default]
             * credential profile by reading from the credentials file located at
             * (~/.aws/credentials).
             */
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

//        AmazonS3ClientBuilder.standard()
        AWSCredentialsProvider awsCredentials = new DefaultAWSCredentialsProviderChain();
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(awsCredentials)
                .withRegion(Constants.DEFAULT_REGION)
                .build();

//        s3 = AmazonS3Client.builder()
//                .withCredentials(AuthHelper.getDefaultPropertiesCredentialsProvider())
//                .withCredentials(AuthHelper.getInstanceProfieCredentialsProvider())
//                .withRegion(Constants.DEFAULT_REGION)
//                .build();
////                .withCredentials(new AWSStaticCredentialsProvider(credentials))
////                .withRegion("us-east-1")
////                .build();
    }

    public static AWSCredentials getCredentials() {
        return credentials;
    }

    public static AmazonS3 getS3() {
        return s3;
    }

    public S3Utils() {
    }

    public S3Utils(String bucket) {
        this.bucket = bucket;
    }

    public S3Utils(String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
    }

    public static void main(String[] args) {
        S3Utils s3Utils = new S3Utils(SAGE_BUCKET);
        try {
            s3Utils.downloadFromS3(JFISH_MKV_KEY, null);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void testS3Apis() throws IOException {

//        AWSCredentials credentials = null;
//        try {
//            credentials = new ProfileCredentialsProvider().getCredentials();
//        } catch (Exception e) {
//            throw new AmazonClientException(
//                    "Cannot load the credentials from the credential profiles file. " +
//                            "Please make sure that your credentials file is at the correct " +
//                            "location (~/.aws/credentials), and is in valid format.",
//                    e);
//        }
//
//        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
//                .withCredentials(new AWSStaticCredentialsProvider(credentials))
//                .withRegion("us-east-1")
//                .build();

        String bucketName = "my-first-s3-bucket-" + UUID.randomUUID();
        String key = "MyObjectKey";

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon S3");
        System.out.println("===========================================\n");

        try {
            /*
             * Create a new S3 bucket - Amazon S3 bucket names are globally unique,
             * so once a bucket name has been taken by any user, you can't create
             * another bucket with that same name.
             *
             * You can optionally specify a location for your bucket if you want to
             * keep your data closer to your applications or users.
             */
            System.out.println("Creating bucket " + bucketName + "\n");
            s3.createBucket(bucketName);

            /*
             * List the buckets in your account
             */
            System.out.println("Listing buckets");
            for (Bucket bucket : s3.listBuckets()) {
                System.out.println(" - " + bucket.getName());
            }
            System.out.println();

            /*
             * Upload an object to your bucket - You can easily upload a file to
             * S3, or upload directly an InputStream if you know the length of
             * the data in the stream. You can also specify your own metadata
             * when uploading to S3, which allows you set a variety of options
             * like content-type and content-encoding, plus additional metadata
             * specific to your applications.
             */
            System.out.println("Uploading a new object to S3 from a file\n");
            s3.putObject(new PutObjectRequest(bucketName, key, createSampleFile()));

            /*
             * Download an object - When you download an object, you get all of
             * the object's metadata and a stream from which to read the contents.
             * It's important to read the contents of the stream as quickly as
             * possibly since the data is streamed directly from Amazon S3 and your
             * network connection will remain open until you read all the data or
             * close the input stream.
             *
             * GetObjectRequest also supports several other options, including
             * conditional downloading of objects based on modification times,
             * ETags, and selectively downloading a range of an object.
             */
            System.out.println("Downloading an object");
            S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
            System.out.println("Content-Type: " + object.getObjectMetadata().getContentType());
            displayTextInputStream(object.getObjectContent());

            /*
             * List objects in your bucket by prefix - There are many options for
             * listing the objects in your bucket.  Keep in mind that buckets with
             * many objects might truncate their results when listing their objects,
             * so be sure to check if the returned object listing is truncated, and
             * use the AmazonS3.listNextBatchOfObjects(...) operation to retrieve
             * additional results.
             */
            System.out.println("Listing objects");
            ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                    .withBucketName(bucketName)
                    .withPrefix("My"));
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                System.out.println(" - " + objectSummary.getKey() + "  " +
                        "(size = " + objectSummary.getSize() + ")");
            }
            System.out.println();

            /*
             * Delete an object - Unless versioning has been turned on for your bucket,
             * there is no way to undelete an object, so use caution when deleting objects.
             */
//            System.out.println("Deleting an object\n");
//            s3.deleteObject(bucketName, key);
//
//            /*
//             * Delete a bucket - A bucket must be completely empty before it can be
//             * deleted, so remember to delete any objects from your buckets before
//             * you try to delete them.
//             */
//            System.out.println("Deleting bucket " + bucketName + "\n");
//            s3.deleteBucket(bucketName);
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    /**
     * Creates a temporary file with text data to demonstrate uploading a file
     * to Amazon S3
     *
     * @return A newly created temporary file with text data.
     * @throws IOException
     */
    private static File createSampleFile() throws IOException {
        File file = File.createTempFile("aws-java-sdk-", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("01234567890112345678901234\n");
        writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
        writer.write("01234567890112345678901234\n");
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.close();

        return file;
    }

    /**
     * Displays the contents of the specified input stream as text.
     *
     * @param input The input stream to display as text.
     * @throws IOException
     */
    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }


    public String downloadFromS3(String key, String downloadFilePath) throws Exception {
//        LogUtils.debug(LOG_TAG, "Downloading file: " + key);
        TransferManager tm = new TransferManager(new DefaultAWSCredentialsProviderChain());
        // TransferManager processes all transfers asynchronously,
        // so this call will return immediately.
        File downloadedFile = null;
        if(StringUtils.isEmpty(downloadFilePath))
            downloadedFile = new File(Constants.S3_DOWNLOAD_DIR + "/" + key);
        else
            downloadedFile = new File(downloadFilePath);

        downloadedFile.getParentFile().mkdirs();
        downloadedFile.createNewFile();
        Download download = tm.download(bucket, key, downloadedFile);
        download.waitForCompletion();

//        LogUtils.debug(LOG_TAG, "Successfully downloaded file from bucket.\nName: " + key + "\nBucket name: " +
//                bucket);
        tm.shutdownNow();
        return downloadedFile.getAbsolutePath();
    }

    public void uploadToS3(String key) throws Exception {
        File file = new File(key);
//        LogUtils.debug(LOG_TAG, "Uploading new file. Name: " + key);
        TransferManager tm = new TransferManager(new DefaultAWSCredentialsProviderChain());
        // TransferManager processes all transfers asynchronously,
        // so this call will return immediately.
        Upload upload = tm.upload(bucket, key, file);
        upload.waitForCompletion();
//        LogUtils.debug(LOG_TAG, "Successfully uploaded. File : " + key + "\nBucket name: " +
//                bucket);
        tm.shutdownNow();
    }

    public void remove(String accessKey) throws Exception {
        LogUtils.debug(LOG_TAG, "Deleting file with access key: " + accessKey);
//        AmazonS3 s3Client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
        AmazonS3 s3Client = getS3();
        DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(bucket);

        List<KeyVersion> keys = new ArrayList<KeyVersion>();
        keys.add(new KeyVersion(accessKey));
        keys.add(new KeyVersion(accessKey + "_key"));

        multiObjectDeleteRequest.setKeys(keys);

        s3Client.deleteObjects(multiObjectDeleteRequest);

        LogUtils.debug(LOG_TAG, "Deleted file with access key: " + accessKey);
    }
}
