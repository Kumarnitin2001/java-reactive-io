package io.github.kn.flow.samples;

import io.github.kn.flow.CompletionPublisher;
import io.github.kn.flow.CompletionPublisherAdapter;
import io.github.kn.flow.CompletionSubscriber;
import io.github.kn.flow.aws.s3.S3ObjectPublisher;
import io.github.kn.flow.aws.sqs.SQSMessagePublishingSubscriberBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Sample code publishing AWS S3 file as SQS messages (each file line is published as a message body
 * {@link SendMessageRequest#messageBody()})
 * <br>
 * <br>
 * <i>Note that the S3 bucket and file object as the SQS queue name are all configured as constants in the code.
 * The AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) and region (AWS_DEFAULT_REGION) values need to be
 * provided externally - as java environment variables - to composeAsync this example. </i>
 */
public class ExampleS3ToSQSMessageFlow {
    private static final Logger LOG = LogManager.getLogger("ExampleS3ToSQSMessageFlow");

    private static final String S3_OBJECT_NAME = "lines1.txt";
    private static final String S3_BUCKET = S3ObjectPublisher.DEFAULT_BUCKET_NAME;
    private static final String SQS_QUEUE = SQSMessagePublishingSubscriberBuilder.DEFAULT_QUEUE_NAME;

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
            CompletionSubscriber<List<ByteBuffer>> sqsMessagingSubscriber = new SQSMessagePublishingSubscriberBuilder().
                    queueName(SQS_QUEUE).client(javaClient)
                    .build();
            s3BytesPublisher.subscribe(sqsMessagingSubscriber).get(10, TimeUnit.MINUTES);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            LOG.error("Exception completing the flow", e);
        }
        LOG.info("Done publishing queue messages for S3 object {} in bucket {}. Verify message in {} ", S3_OBJECT_NAME
                , S3ObjectPublisher.DEFAULT_BUCKET_NAME, SQSMessagePublishingSubscriberBuilder.DEFAULT_QUEUE_NAME);
    }
}
