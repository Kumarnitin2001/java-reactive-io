package io.github.kn.flow.aws.sqs;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.InvalidMessageContentsException;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

/**
 *
 */
class SQSPublishingItemProcessorTest {


    private SQSPublishingItemProcessor sqsPublisher;
    private MockSQSAsyncClient mockClientDelegate;

    @BeforeEach
    void setUp() {
        mockClientDelegate = new MockSQSAsyncClient();
        sqsPublisher = new SQSPublishingItemProcessor(mockClientDelegate, "testQueue");
    }

    @Test
    void queueUrlFound() throws Exception {
        mockClientDelegate
                .setGetQueueUrlResponse(CompletableFuture
                        .completedFuture(GetQueueUrlResponse.builder().queueUrl("testQueueUrl").build()));
        sqsPublisher.prepare().toCompletableFuture().get();
        Assertions.assertEquals("testQueue", mockClientDelegate.getGetQueueUrlArg().queueName());
        invokeOnNext(List.of("testMessage"));
        Assertions.assertEquals("testQueueUrl", mockClientDelegate.getSendMessageBatchArg().queueUrl());
    }

    private CompletionStage<Stream<SQSPublishingItemProcessor.ResponseEntry>> invokeOnNext(List<String> messages) {
        return sqsPublisher.onNext(messages.stream());
    }

    @Test
    void queueUrlNotFoundCreatesNewQueue() throws Exception {
        mockClientDelegate
                .setGetQueueUrlResponse(CompletableFuture
                        .failedFuture(QueueDoesNotExistException.builder().build()));
        mockClientDelegate
                .setCreateQueueResponse(CompletableFuture
                        .completedFuture(CreateQueueResponse.builder().queueUrl("testQueueUrl").build()));
        sqsPublisher.prepare().toCompletableFuture().get();
        Assertions.assertEquals("testQueue", mockClientDelegate.getCreateQueueArg().queueName());
        invokeOnNext(List.of("testMessage"));
        Assertions.assertEquals("testQueueUrl", mockClientDelegate.getSendMessageBatchArg().queueUrl());
    }

    @Test
    void onInvalidMessage() {
        mockClientDelegate.setBatchCallException(InvalidMessageContentsException.builder().build());
        Throwable t = new Throwable();
        try {
            invokeOnNext(List.of(""))
                    .toCompletableFuture().get();
        } catch (Exception e) {
            t = e.getCause();
        }
        Assertions.assertEquals(t.getClass(), IllegalArgumentException.class);
    }

    @Test
    void onSendMessageException() {
        mockClientDelegate.setBatchCallException(new RuntimeException());
        Assertions.assertThrows(ExecutionException.class, () -> invokeOnNext(List.of(""))
                .toCompletableFuture().get());
    }

    @Test
    void onNextEmptyStream() throws Exception {
        Assertions.assertTrue(invokeOnNext(List.of()).toCompletableFuture().get().findAny().isEmpty());
        Assertions.assertNull(mockClientDelegate.getSendMessageBatchArg());

    }

    @Test
    void onNextSuccess() throws Exception {

        List<SQSPublishingItemProcessor.ResponseEntry> response = invokeOnNext(List.of("testMessage"))
                .toCompletableFuture().get().collect(Collectors.toList());
        Assertions.assertEquals(1, response.size());
        Assertions.assertTrue(response.get(0).isSuccess());
        Assertions.assertEquals("SendMessageBatchResultEntry(Id=0)", response.get(0).message());
        Assertions.assertEquals(1, mockClientDelegate.getSendMessageBatchArg().entries().size());
        Assertions.assertEquals("testMessage", mockClientDelegate.getSendMessageBatchArg().entries().get(0)
                .messageBody());
    }

    @Test
    void onNextError() throws Exception {
        mockClientDelegate.addBatchResponseError(BatchResultErrorEntry.builder().id("0").build());
        List<SQSPublishingItemProcessor.ResponseEntry> response = invokeOnNext(List.of("testMessage"))
                .toCompletableFuture().get().collect(Collectors.toList());
        Assertions.assertEquals(1, response.size());
        Assertions.assertFalse(response.get(0).isSuccess());
        Assertions.assertEquals("BatchResultErrorEntry(Id=0)", response.get(0).message());
    }


    @Test
    void onNextSendTwoMessages() throws Exception {
        invokeOnNext(List.of("testMessage1", "testMessage2")).toCompletableFuture().get();
        Assertions.assertEquals(2, mockClientDelegate.getSendMessageBatchArg().entries().size());
        Assertions.assertEquals("testMessage1", mockClientDelegate.getSendMessageBatchArg().entries().get(0)
                .messageBody());
        Assertions.assertEquals("testMessage2", mockClientDelegate.getSendMessageBatchArg().entries().get(1)
                .messageBody());
    }

    @Test
    void toStringMethod() {
        Assertions.assertEquals("testQueue", sqsPublisher.toString());
    }

    @Test
    void sendMessageMixFailures() throws Exception {
        mockClientDelegate.addBatchResponseError(BatchResultErrorEntry.builder().id("0").build())
                .addBatchResponseError(BatchResultErrorEntry.builder().id("2").build());
        Assertions
                .assertEquals("[BatchResultErrorEntry(Id=0), SendMessageBatchResultEntry(Id=1), BatchResultErrorEntry" +
                        "(Id=2), SendMessageBatchResultEntry(Id=3)]", invokeOnNext(List
                        .of("","", "", "")).toCompletableFuture().get().collect(Collectors.toList()).toString());
    }
}