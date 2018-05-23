package com.amazonaws.kinesisvideo.demoapp.utils;

import net.bramp.ffmpeg.FFmpeg;
import net.bramp.ffmpeg.FFmpegExecutor;
import net.bramp.ffmpeg.FFprobe;
import net.bramp.ffmpeg.builder.FFmpegBuilder;

import java.io.IOException;

public class FFMPEGUtils {

    private static final String ffmpegPath = System.getProperty("ffmpeg.path", "/usr/local/bin/ffmpeg");
    private static final String ffprobePath = System.getProperty("ffprobe.path", "/usr/local/bin/ffprobe");

    private static String sampleMP4InputFile = "/home/upen/aws/s3/sage-media-bucket/demo/mp4/BigBuckBunny_320x180.mp4";
    private static String sampleOutputMKVFile = "/tmp/BigBuckBunny_320x180.mkv";

    private FFmpeg ffmpeg = null;
    private FFprobe ffprobe = null;

    public FFMPEGUtils() throws IOException {
        initialise();
    }

    public static void main(String[] args) {
        try {
            FFMPEGUtils ffmpegUtils = new FFMPEGUtils();
            ffmpegUtils.convert(sampleMP4InputFile, sampleOutputMKVFile);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    private void initialise() throws IOException {
        ffmpeg = new FFmpeg(ffmpegPath);
        ffprobe = new FFprobe(ffprobePath);
    }

    public void convert(String inputPath, String outputPath) {
        FFmpegBuilder builder = new FFmpegBuilder()
                .setInput(inputPath)     // Filename, or a FFmpegProbeResult
                .overrideOutputFiles(true) // Override the output if it exists
                .addOutput(outputPath)   // Filename for the destination
//                .setFormat("mp4")        // Format is inferred from filename, or can be set
//                .setTargetSize(250_000)  // Aim for a 250KB file
                .disableSubtitle()       // No subtiles
                .setAudioChannels(1)         // Mono audio
                .setAudioCodec("aac")        // using the aac codec
                .setAudioSampleRate(48_000)  // at 48KHz
                .setAudioBitRate(32768)      // at 32 kbit/s
//                .setVideoCodec("libx264")     // Video using x264
                .setVideoFrameRate(24, 1)     // at 24 frames per second
                .setVideoResolution(640, 480) // at 640x480 resolution
                .setStrict(FFmpegBuilder.Strict.EXPERIMENTAL) // Allow FFmpeg to use experimental specs
                .done();

        FFmpegExecutor executor = new FFmpegExecutor(ffmpeg, ffprobe);

// Run a one-pass encode
        executor.createJob(builder).run();

// Or run a two-pass encode (which is slower at the cost of better quality)
        executor.createTwoPassJob(builder).run();
    }
}
