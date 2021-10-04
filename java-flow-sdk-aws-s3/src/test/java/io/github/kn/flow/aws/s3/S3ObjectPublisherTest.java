package io.github.kn.flow.aws.s3;


import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.kn.flow.util.MockCompletionSubscriber;
import io.github.kn.flow.util.MockHttpClient;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.builder.SdkDefaultClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

class S3ObjectPublisherTest {

    private S3ObjectPublisher publisher;

    private MockHttpClient mockHttpClient;

    private AtomicReference<Flow.Subscriber<? super List<ByteBuffer>>> subscriberReference;

    private MockCompletionSubscriber<List<ByteBuffer>> mockCompletionSubscriber;

    @BeforeEach
    void setUp() {
        mockHttpClient = new MockHttpClient();
        subscriberReference = new AtomicReference<>();
        mockCompletionSubscriber = new MockCompletionSubscriber<>();
        publisher = new S3ObjectPublisher("testS3Object", "testBucket", S3AsyncClient.builder()
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .endpointOverride(URI.create("http://testURI"))
                .region(Region.AP_SOUTH_1), mockHttpClient, subscriberReference);
    }

    @Test
    void subscribeInvokesHttpBodySubscription() {
        AtomicReference<Flow.Subscriber<? super List<ByteBuffer>>> localRef = new AtomicReference<>();
        mockHttpClient.setBody((Flow.Publisher<Object>) subscriber ->
                localRef.set(subscriber)
        );
        publisher.subscribe(mockCompletionSubscriber);
        Assertions.assertSame(mockCompletionSubscriber, localRef.get());
    }

    @Test
    void httpRequestURI() {
        publisher.subscribe(mockCompletionSubscriber);
        Assertions.assertEquals(URI.create("http://testURI/testBucket/testS3Object"), mockHttpClient.getRequestArg()
                .uri());
    }

    @Test
    void builderWithClient() {
        Assertions.assertNotNull(S3ObjectPublisher.builder("testObject").setJavaClient(new MockHttpClient())
                .s3ClientBuilder(new MockS3AsyncClientBuilder()).setBucketName("testBucket").build());
    }

    @Test
    void builderNoClient() {
        Assertions.assertNotNull(S3ObjectPublisher.builder("testObject")
                .s3ClientBuilder(new MockS3AsyncClientBuilder()).setBucketName("testBucket").build());
    }

    private class MockS3AsyncClientBuilder extends SdkDefaultClientBuilder<S3AsyncClientBuilder, S3AsyncClient> implements S3AsyncClientBuilder {

        @Override
        public S3AsyncClientBuilder credentialsProvider(AwsCredentialsProvider awsCredentialsProvider) {
            return this;
        }

        @Override
        public S3AsyncClientBuilder region(Region region) {
            return this;
        }

        @Override
        protected S3AsyncClient buildClient() {
            return new S3AsyncClient() {
                @Override
                public String serviceName() {
                    return null;
                }

                @Override
                public void close() {

                }
            };
        }

        @Override
        public S3AsyncClientBuilder serviceConfiguration(S3Configuration s3Configuration) {
            return this;
        }
    }

}