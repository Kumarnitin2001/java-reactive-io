package io.github.kn.flow.samples;

import io.github.kn.flow.CompletionPublisher;
import io.github.kn.flow.CompletionPublisherAdapter;
import io.github.kn.flow.CompletionSubscriber;
import io.github.kn.flow.CompletionSubscriberImpl;
import io.github.kn.flow.aws.s3.S3ObjectPublisher;
import io.github.kn.flow.file.FileWritingItemProcessor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Sample code downloading AWS S3 object locally.
 * <br>
 * <br>
 * <i>Note that the S3 bucket and file object as the local file path are all configured as constants in the code.
 * The AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) and region (AWS_DEFAULT_REGION) values need to be
 * provided externally - as java environment variables - to composeAsync this example. </i>
 */
public class ExampleS3ToLocalFileFlow {
    private static final Logger LOG = LogManager.getLogger("ExampleS3ToLocalFileFlow");
    private static final String S3_OBJECT_NAME = "lines1.txt";
    private static final String LOCAL_FILE_PATH = "Downloaded.txt";
    private static final String S3_BUCKET = S3ObjectPublisher.DEFAULT_BUCKET_NAME;

    public static void main(String[] args) {
        HttpClient javaClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(200l))
                .executor(ForkJoinPool.commonPool())
                .build();

        try {
            CompletionPublisher<List<ByteBuffer>> s3BytesPublisher = CompletionPublisherAdapter
                    .adapt(S3ObjectPublisher.builder(S3_OBJECT_NAME).setBucketName(S3_BUCKET).setJavaClient(javaClient)
                            .build());
            CompletionSubscriber<List<ByteBuffer>> fileWritingSubscriber = CompletionSubscriberImpl
                    .wrap(new FileWritingItemProcessor(Paths
                            .get(LOCAL_FILE_PATH)), 1, 1);
            s3BytesPublisher.subscribe(fileWritingSubscriber).get(10, TimeUnit.MINUTES);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            LOG.error("Exception completing the flow", e);
        }
        LOG.info("Done downloading S3 object {} in bucket {}. Verify contents in {} ", S3_OBJECT_NAME
                , S3ObjectPublisher.DEFAULT_BUCKET_NAME, LOCAL_FILE_PATH);
    }
}
