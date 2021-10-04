package io.github.kn.flow.aws.sqs;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.github.kn.flow.util.MockHttpClient;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.builder.SdkDefaultClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder;

/**
 *
 */
class SQSMessagePublishingSubscriberBuilderTest {


    @Test
    public void testBuildCustomized() {
        Assertions.assertNotNull(new SQSMessagePublishingSubscriberBuilder()
                .sqsClientBuilder(new MockSqsAsyncClientBuilder()).client(new MockHttpClient()).backPressureChunkSize(1)
                .charset(StandardCharsets.UTF_8).maxConcurrency(1).messageSeparator(";").queueName("testQueueName")
                .executor((r) -> r.run()).build());
    }

    @Test
    public void testBuildWithDefaults() {
        Assertions.assertNotNull(new SQSMessagePublishingSubscriberBuilder()
                .sqsClientBuilder(new MockSqsAsyncClientBuilder())
                .queueName("testQueueName")
                .build());
    }

    private class MockSqsAsyncClientBuilder extends SdkDefaultClientBuilder<SqsAsyncClientBuilder, SqsAsyncClient> implements SqsAsyncClientBuilder {

        @Override
        public SqsAsyncClientBuilder credentialsProvider(AwsCredentialsProvider awsCredentialsProvider) {
            return this;
        }

        @Override
        public SqsAsyncClientBuilder region(Region region) {
            return this;
        }

        @Override
        protected SqsAsyncClient buildClient() {
            return new SqsAsyncClient() {
                @Override
                public String serviceName() {
                    return null;
                }

                @Override
                public void close() {

                }
            };
        }
    }
}