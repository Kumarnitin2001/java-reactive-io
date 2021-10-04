package io.github.kn.flow.aws.s3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.github.kn.flow.aws.AbstractJavaNativeSdkHttpClient;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implements a generic S3 Object Bytes(Buffer) {@link java.util.concurrent.Flow.Publisher}, publishing
 * object bytes to the subscribed {@link java.util.concurrent.Flow.Subscriber}
 */
public class S3ObjectPublisher implements Flow.Publisher<List<ByteBuffer>> {

    public static final String DEFAULT_BUCKET_NAME = "textline-publisher-files";
    private static final Logger LOG = LogManager.getLogger("S3ObjectPublisher");
    private final S3AsyncClient s3AsyncClient;
    private final String bucketName;
    private final String objectName;
    private final AtomicReference<Flow.Subscriber<? super List<ByteBuffer>>> byteBufferSubscriberReference;

    /**
     * @param client
     * @param name
     * @param subscriberReference
     */
    S3ObjectPublisher(
            final S3AsyncClient client, final String name, final String bucket,
            final AtomicReference<Flow.Subscriber<? super List<ByteBuffer>>> subscriberReference) {
        this.objectName = name;
        this.s3AsyncClient = client;
        this.bucketName = bucket;
        this.byteBufferSubscriberReference = subscriberReference;
    }

    /**
     * @param name
     * @param javaClient
     */
    S3ObjectPublisher(final String name, final String bucket, final HttpClient javaClient) {
        this(name, bucket, javaClient, new AtomicReference<>());
    }

    /**
     * @param name
     * @param javaClient
     */
    S3ObjectPublisher(final String name, final HttpClient javaClient) {
        this(name, DEFAULT_BUCKET_NAME, javaClient);
    }

    /**
     * @param name
     * @param javaHttpClient
     * @param subscriberReference
     */
    S3ObjectPublisher(final String name,
                      final String bucket,
                      final HttpClient javaHttpClient,
                      final AtomicReference<Flow.Subscriber<? super List<ByteBuffer>>> subscriberReference) {
        this(name, bucket, S3AsyncClient.builder(), javaHttpClient, subscriberReference);
    }

    S3ObjectPublisher(final String name,
                      final String bucket,
                      final S3AsyncClientBuilder s3AsyncClientBuilder,
                      final HttpClient javaHttpClient,
                      final AtomicReference<Flow.Subscriber<? super List<ByteBuffer>>> subscriberReference) {
        this(s3AsyncClientBuilder.asyncConfiguration(ClientAsyncConfiguration.builder()
                .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, ForkJoinPool.commonPool())
                .build()).httpClient(new AbstractJavaNativeSdkHttpClient() {
            @Override
            public CompletableFuture<Void> execute(final AsyncExecuteRequest asyncExecuteRequest) {
                HttpResponse.BodyHandler<Flow.Publisher<List<ByteBuffer>>> bodyHandler =
                        rspInfo ->
                                HttpResponse.BodySubscribers.ofPublisher();
                return prepareBytePublisherBodyHandler(javaHttpClient
                        .sendAsync(adapt(asyncExecuteRequest), bodyHandler), asyncExecuteRequest.responseHandler())
                        .thenAccept(r -> {
                                    LOG.info("Response fromHttpRequest S3 for request {} is {} with response headers " +
                                                    "{} ",
                                            r.request(), r
                                                    .statusCode(), r.headers());
                                    r.body().subscribe(subscriberReference.get());
                                    LOG.trace("Subscribed successfully to  S3Object {} of bucket {}", name, bucket);
                                }
                        );
            }
        }).build(), name, bucket, subscriberReference);
    }

    /**
     * @param objectName Name of the S3 object to publish bytes of.
     * @return
     */
    public static Builder builder(final String objectName) {
        return new Builder(objectName);
    }

    @Override
    public void subscribe(final Flow.Subscriber<? super List<ByteBuffer>> byteBufferSubscriber) {
        LOG.info("Fetching S3 file {} in bucketName {}", objectName, bucketName);
        this.byteBufferSubscriberReference.set(byteBufferSubscriber);
        // Start the call to Amazon S3, not blocking to wait for the result
        s3AsyncClient.getObject(GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(objectName)
                        .build(),
                new AsyncResponseTransformer<>() {
                    @Override
                    public CompletableFuture<Object> prepare() {
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public void onResponse(GetObjectResponse getObjectResponse) {
                        LOG.trace("onResponse invoked for bucket {} and object {}, Response {}", bucketName,
                                objectName, getObjectResponse);
                    }

                    @Override
                    public void onStream(SdkPublisher<ByteBuffer> sdkPublisher) {
                        LOG.trace("onStream invoked for bucket {} and object {}, Response {}", bucketName,
                                objectName);
                    }

                    @Override
                    public void exceptionOccurred(Throwable throwable) {
                        LOG.error("exceptionOccurred for bucket:" + bucketName + " and object:" + objectName,
                                throwable);
                    }
                });
        LOG.trace("GetObject invoked successfully {}", objectName);
    }

    public static class Builder {
        private volatile String objectName;
        private volatile String bucketName = DEFAULT_BUCKET_NAME;
        private volatile Optional<HttpClient> javaClient = Optional.empty();
        private volatile S3AsyncClientBuilder s3AsyncClientBuilder = S3AsyncClient.builder();

        private Builder(final String objName) {
            this.objectName = objName;
        }

        public Builder setBucketName(final String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public Builder setJavaClient(final HttpClient javaClient) {
            this.javaClient = Optional.of(javaClient);
            return this;
        }

        public Builder s3ClientBuilder(final S3AsyncClientBuilder builder) {
            this.s3AsyncClientBuilder = builder;
            return this;
        }

        public S3ObjectPublisher build() {
            return new S3ObjectPublisher(objectName, bucketName, s3AsyncClientBuilder, javaClient
                    .orElseGet(() -> HttpClient.newBuilder()
                            .version(HttpClient.Version.HTTP_1_1)
                            .followRedirects(HttpClient.Redirect.NORMAL).connectTimeout(Duration.ofSeconds(200l))
                            .executor(ForkJoinPool.commonPool())
                            .build()), new AtomicReference<>());
        }
    }
}