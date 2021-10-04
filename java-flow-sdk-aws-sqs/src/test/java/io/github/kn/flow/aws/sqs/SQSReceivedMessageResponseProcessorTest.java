package io.github.kn.flow.aws.sqs;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.kn.flow.Optionals;
import io.github.kn.flow.util.MockCompletionStageItemProcessor;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/**
 *
 */
class SQSReceivedMessageResponseProcessorTest {


    private SQSReceivedMessageResponseProcessor processor;
    private MockSQSAsyncClient mockSQSAsyncClient;

    private MockCompletionStageItemProcessor<Optionals<String>, Optionals<?>> mockCompletionStageItemProcessor;

    @BeforeEach
    public void setUp() {
        mockSQSAsyncClient = new MockSQSAsyncClient();
        mockCompletionStageItemProcessor = new MockCompletionStageItemProcessor<>();
        processor = new SQSReceivedMessageResponseProcessor(mockSQSAsyncClient, "testQueueName",
                mockCompletionStageItemProcessor);
    }

    @Test
    void prepare() {
        mockCompletionStageItemProcessor.setPrepareCompletionStage(CompletableFuture.completedStage(null));
        mockSQSAsyncClient
                .setGetQueueUrlResponse(CompletableFuture.completedFuture(GetQueueUrlResponse.builder().build()));
        processor.prepare();
        Assertions.assertEquals(1, mockCompletionStageItemProcessor.getTimesPrepareInvoked());
    }

    @Test
    void onNextBlankMessage() {
        processor.onNext(ReceiveMessageResponse.builder().messages(Message.builder().body("").build()).build());
        Assertions.assertNull(mockCompletionStageItemProcessor.getOnNextItem());
    }

    @Test
    void onNextEmptyMessage() {
        processor.onNext(ReceiveMessageResponse.builder().messages(Message.builder().build()).build());
        Assertions.assertNull(mockCompletionStageItemProcessor.getOnNextItem());
    }

    @Test
    void onNextNoMessage() {
        processor.onNext(ReceiveMessageResponse.builder().build());
        Assertions.assertNull(mockCompletionStageItemProcessor.getOnNextItem());
    }

    @Test
    void messageProcessorException() {
        mockCompletionStageItemProcessor.setOnNextException(new RuntimeException());
        Assertions
                .assertTrue(processor.onNext(ReceiveMessageResponse.builder()
                        .messages(Message.builder().body("testMessage").build())
                        .build())
                        .toCompletableFuture().isCompletedExceptionally());
    }


    @Test
    void messageProcessorCompletionException() {
        mockCompletionStageItemProcessor
                .setOnNextResponse(CompletableFuture.failedStage(new RuntimeException()));
        Assertions
                .assertTrue(processor.onNext(ReceiveMessageResponse.builder()
                        .messages(Message.builder().body("testMessage").build())
                        .build())
                        .toCompletableFuture().isCompletedExceptionally());
    }

    @Test
    void onNextOneMessage() {
        mockCompletionStageItemProcessor
                .setOnNextResponse(CompletableFuture.completedFuture(Optionals.create(List.of(""))));
        processor.onNext(ReceiveMessageResponse.builder()
                .messages(Message.builder().body("testMessage").receiptHandle("testReceiptHandle").build())
                .build());
        Assertions.assertEquals("testMessage", mockCompletionStageItemProcessor.getOnNextItem().toString());
        Assertions
                .assertEquals("[DeleteMessageBatchRequestEntry(Id=1, ReceiptHandle=testReceiptHandle)]",
                        mockSQSAsyncClient
                                .getDeleteMessageBatchArg()
                                .entries().toString());
        Assertions.assertEquals(1, mockSQSAsyncClient.getDeleteMessageBatchArg().entries().size());
    }

    @Test
    void onNextTwoMessagesWithBothSuccess() {
        mockCompletionStageItemProcessor
                .setOnNextResponse(CompletableFuture
                        .completedFuture(Optionals.wrap(List.of(Optional.of(""), Optional.of("")))));
        processor.onNext(ReceiveMessageResponse.builder()
                .messages(Message.builder().body("testMessage1").build(), Message.builder().body("testMessage2")
                        .build())
                .build());
        Assertions.assertEquals(2, mockSQSAsyncClient.getDeleteMessageBatchArg().entries().size());
    }

    @Test
    void onNextTwoMessagesWithOneFailure() {
        mockCompletionStageItemProcessor
                .setOnNextResponse(CompletableFuture
                        .completedFuture(Optionals.wrap(List.of(Optional.of(""), Optional.empty()))));
        processor.onNext(ReceiveMessageResponse.builder()
                .messages(Message.builder().body("testMessage1").build(), Message.builder().body("testMessage2")
                        .build())
                .build());
        Assertions.assertEquals(1, mockSQSAsyncClient.getDeleteMessageBatchArg().entries().size());
    }
}