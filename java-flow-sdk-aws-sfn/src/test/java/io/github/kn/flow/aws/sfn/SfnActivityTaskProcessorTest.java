package io.github.kn.flow.aws.sfn;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.kn.flow.aws.sfn.AbstractSfnProcessor.SimpleJsonInputOutput;
import io.github.kn.flow.util.MockCompletionStageItemProcessor;
import software.amazon.awssdk.services.sfn.SfnAsyncClient;
import software.amazon.awssdk.services.sfn.model.GetActivityTaskResponse;
import software.amazon.awssdk.services.sfn.model.SendTaskFailureRequest;
import software.amazon.awssdk.services.sfn.model.SendTaskFailureResponse;
import software.amazon.awssdk.services.sfn.model.SendTaskSuccessRequest;
import software.amazon.awssdk.services.sfn.model.SendTaskSuccessResponse;

/**
 *
 */
class SfnActivityTaskProcessorTest {

    SfnActivityTaskProcessor<SimpleJsonInputOutput, SimpleJsonInputOutput> processor;

    MockCompletionStageItemProcessor<String, String> mockItemProcessor;

    MockSfnAsyncClient client;

    private GetActivityTaskResponse createActivityTask(final String inp, final String token) {
        return GetActivityTaskResponse.builder().input(inp).taskToken(token).build();
    }

    @BeforeEach
    void setUp() {
        client = new MockSfnAsyncClient();
        mockItemProcessor = new MockCompletionStageItemProcessor<String, String>()
                .setOnNextResponse(CompletableFuture.completedFuture("testResponse"));
        processor = SfnActivityTaskProcessor.builder(mockItemProcessor).client(client).build();
    }

    @Test
    void prepare() {
    }

    @Test
    void onNextInvokesItemProcessor() {
        processor.onNext(createActivityTask("{\"input\": \"testInput\"}", "testToken"));
        Assertions.assertEquals("testInput", mockItemProcessor.getOnNextItem());
    }

    @Test
    void onNextSuccessCompletesTaskSuccessfully() {
        processor.onNext(createActivityTask("{\"input\": \"testInput\"}", "testToken"));
        Assertions.assertEquals("testToken", client.getTaskSuccessArg().taskToken());
        Assertions.assertEquals("{\"output\":\"testResponse\",\"input\":\"testInput\"}", client.getTaskSuccessArg()
                .output());
    }

    @Test
    void onNextFailureCompletesTasksWithFailure() {
        mockItemProcessor.setOnNextResponse(CompletableFuture.failedFuture(new IllegalArgumentException("testError")));
        processor.onNext(createActivityTask("{\"input\": \"testInput\"}", "testToken"));
        Assertions.assertNull(client.getTaskSuccessArg());
        Assertions.assertEquals("java.lang.IllegalArgumentException: testError", client.getTaskFailureArg()
                .error());
    }

    private class MockSfnAsyncClient implements SfnAsyncClient {
        private CompletableFuture<SendTaskFailureResponse> taskFailureResponse = CompletableFuture
                .completedFuture(SendTaskFailureResponse.builder().build());
        private CompletableFuture<SendTaskSuccessResponse> taskSuccessResponse = CompletableFuture
                .completedFuture(SendTaskSuccessResponse.builder().build());
        private SendTaskFailureRequest taskFailureArg;
        private SendTaskSuccessRequest taskSuccessArg;

        public MockSfnAsyncClient setTaskFailureResponse(CompletableFuture<SendTaskFailureResponse> taskFailureResponse) {
            this.taskFailureResponse = taskFailureResponse;
            return this;
        }

        public MockSfnAsyncClient setTaskSuccessResponse(CompletableFuture<SendTaskSuccessResponse> taskSuccessResponse) {
            this.taskSuccessResponse = taskSuccessResponse;
            return this;
        }

        public SendTaskFailureRequest getTaskFailureArg() {
            return taskFailureArg;
        }

        public SendTaskSuccessRequest getTaskSuccessArg() {
            return taskSuccessArg;
        }

        @Override
        public CompletableFuture<SendTaskFailureResponse> sendTaskFailure(SendTaskFailureRequest sendTaskFailureRequest) {
            this.taskFailureArg = sendTaskFailureRequest;
            return this.taskFailureResponse;
        }

        @Override
        public CompletableFuture<SendTaskSuccessResponse> sendTaskSuccess(SendTaskSuccessRequest sendTaskSuccessRequest) {
            this.taskSuccessArg = sendTaskSuccessRequest;
            return this.taskSuccessResponse;
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