package io.github.kn.flow.aws.sfn;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.sfn.SfnAsyncClient;
import software.amazon.awssdk.services.sfn.model.StartExecutionRequest;
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse;

/**
 *
 */
class SfnInitiatingProcessorTest {

    private SfnInitiatingProcessor<String> processor;
    private MockSfnAsyncClient mockSfnClient;

    @BeforeEach
    void setUp() {
        mockSfnClient = new MockSfnAsyncClient()
                .setStartExecutionResult(CompletableFuture.completedFuture(StartExecutionResponse.builder().build()));
        processor = SfnInitiatingProcessor.<String>builder("testSMARN").client(mockSfnClient).build();
    }

    @Test
    void onNext() {
        processor.onNext("testInput");
        Assertions.assertEquals("testSMARN", mockSfnClient.getStartExecutionRequestArg().stateMachineArn());
        Assertions.assertEquals("{\"output\":null,\"input\":\"testInput\"}", mockSfnClient.getStartExecutionRequestArg()
                .input());
    }

    @Test
    void buildWithDefaults() {
        Assertions.assertNotNull(SfnInitiatingProcessor.builder("testARN").client(mockSfnClient).build());
    }

    @Test
    void buildCustom() {
        Assertions.assertNotNull(SfnInitiatingProcessor.builder("testARN").client(mockSfnClient)
                .executor(r -> r.run())
                .build());
    }

    private class MockSfnAsyncClient implements SfnAsyncClient {

        private StartExecutionRequest startExecutionRequestArg;
        private CompletableFuture<StartExecutionResponse> startExecutionResult;

        public StartExecutionRequest getStartExecutionRequestArg() {
            return startExecutionRequestArg;
        }

        public MockSfnAsyncClient setStartExecutionResult(CompletableFuture<StartExecutionResponse> startExecutionResult) {
            this.startExecutionResult = startExecutionResult;
            return this;
        }

        @Override
        public CompletableFuture<StartExecutionResponse> startExecution(StartExecutionRequest startExecutionRequest) {
            this.startExecutionRequestArg = startExecutionRequest;
            return startExecutionResult;
        }

        @Override
        public String serviceName() {
            return null;
        }

        @Override
        public void close() {

        }
    }
}