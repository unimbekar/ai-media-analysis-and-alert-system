package com.example.aws.kinesisvideo.consumer.frame;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.example.aws.kinesisvideo.base.AbstractFrameExtractorConsumer;
import com.example.aws.kinesisvideo.base.AbstractKinesisVideoConsumer;
import com.example.aws.kinesisvideo.image.utils.ImageUtils;
import com.example.aws.kinesisvideo.utils.s3.S3Utils;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class KinesisFrameConsumerToS3Sender extends AbstractFrameExtractorConsumer {

	private static final String FRAME_DIRECTORY = System.getProperty("frame.directory", "images");
	private static final String FRAME_FILE_PREFIX = System.getProperty("frame.file.prefix", "frame-");
	// jpg, jpeg, bmp, png etc.
	private static final String FRAME_FILE_FORMAT = System.getProperty("frame.file.format", "jpg");
	private int frameCount = 1;

	public KinesisFrameConsumerToS3Sender() {
		super();
		File directory = new File(FRAME_DIRECTORY);
		if (!directory.exists()) {
			directory.mkdirs();
		}
	}


	protected void process01(BufferedImage imageFrame) {
		String fileName = FRAME_DIRECTORY + "/" + FRAME_FILE_PREFIX + frameCount + "." + FRAME_FILE_FORMAT;
		File outputfile = new File(fileName);
		try {
			ImageIO.write(imageFrame, FRAME_FILE_FORMAT, outputfile);
			S3Utils.getS3()
					.putObject(new PutObjectRequest("rekog-images-bucket", outputfile.getName(), outputfile));
			LOG.info("Frame saved: " + fileName);
			frameCount++;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

    @Override
    protected void process(BufferedImage imageFrame) {
	    sendImageToS3(imageFrame, frameCount++);
    }

    private static void sendImageToS3(BufferedImage bufferedImage, int frameCount) {
        byte[] bytes = ImageUtils.getImageAsByteArr(bufferedImage);
        InputStream inputStream = new ByteArrayInputStream(bytes);

        if (bytes == null) {
            LOG.warn("Could NOT Send the Image .. Found NULL");
            return;
        }

        ObjectMetadata objectMetadata = new ObjectMetadata();
//        objectMetadata.setContentLength(bytes.length);
        objectMetadata.setContentType("image.jpg");
        S3Utils.getS3()
                .putObject(new PutObjectRequest("rekog-images-bucket",
                        "img"+frameCount+".jpg",
                        inputStream,
                        objectMetadata));

        LOG.info("Sending FRAME# " + frameCount);
    }

	@Override
	protected void quit() {
	}

	public static void main(final String[] args) throws Exception {
		AbstractKinesisVideoConsumer consumer = new KinesisFrameConsumerToS3Sender();
		consumer.getMediaLoop();
	}

}
