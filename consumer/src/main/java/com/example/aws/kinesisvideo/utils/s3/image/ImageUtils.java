package com.example.aws.kinesisvideo.utils.s3.image;

import org.json.JSONObject;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;

public class ImageUtils {

    private static String imagePath = "data/images/frame-1.jpg";

    public static void main(String[] args) {
        byte[] arr = getImageAsByteArr(imagePath);
    }

    private File getImageFile(String filePath) {
        return new File(getClass().getClassLoader().getResource(imagePath).getFile());
    }

    public static byte[] getImageAsByteArr(BufferedImage originalImage) {
        byte[] imageInBytes = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(originalImage, "jpg", baos);
            baos.flush();
            imageInBytes = baos.toByteArray();
            baos.close();

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } finally {
            return imageInBytes;
        }
    }

    public static byte[] getImageAsByteArr(String filePath) {
        byte[] imageInBytes = null;
        try {
            imageInBytes = getImageAsByteArr(getBufferedImage(filePath));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } finally {
            return imageInBytes;
        }
    }

    public static BufferedImage getBufferedImage(String filePath) {
        BufferedImage bufferedImage = null;
        try {
            File f = new ImageUtils().getImageFile(imagePath);
            bufferedImage = ImageIO.read(f);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } finally {
            return bufferedImage;
        }
    }

    public static JSONObject encodeImageForKinesisPut(long approximateCaptureTimeinSecs,
                                                      int frameCount,
                                                      byte[] bytes) {

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("ApproximateCaptureTime", approximateCaptureTimeinSecs);
//        jsonObject.put("ImageBytes", org.apache.commons.codec.binary.Base64.encodeBase64String(bytes));
        jsonObject.put("ImageBytes", bytes);
        jsonObject.put("FrameCount", frameCount);


        return jsonObject;
    }
}
