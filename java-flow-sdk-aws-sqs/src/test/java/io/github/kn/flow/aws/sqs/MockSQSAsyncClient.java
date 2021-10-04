package io.github.kn.flow.aws.sqs;

import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 *
 */
public class MockSQSAsyncClient implements SqsAsyncClient {

    private CompletableFuture<CreateQueueResponse> createQueueResponse;
    private CompletableFuture<GetQueueUrlResponse> getQueueUrlResponse;
    private CompletableFuture<SendMessageResponse> sendMessageResponse;

    private Map<String, BatchResultErrorEntry> batchResponseErrors = new HashMap<>();
    private Optional<Exception> batchCallException = Optional.empty();
    private CreateQueueRequest createQueueArg;
    private GetQueueUrlRequest getQueueUrlArg;
    private SendMessageRequest sendMessageArg;
    private DeleteMessageBatchRequest deleteMessageBatchArg;
    private SendMessageBatchRequest sendMessageBatchArg;
    private CompletableFuture<ReceiveMessageResponse> receiveMessageResponse;
    private ReceiveMessageRequest receiveMessageRequestArg;

    public MockSQSAsyncClient setBatchCallException(Exception batchCallException) {
        this.batchCallException = Optional.of(batchCallException);
        return this;
    }

    public MockSQSAsyncClient setReceiveMessageResponse(CompletableFuture<ReceiveMessageResponse> receiveMessageResponse) {
        this.receiveMessageResponse = receiveMessageResponse;
        return this;
    }

    public ReceiveMessageRequest getReceiveMessageRequestArg() {
        return receiveMessageRequestArg;
    }

    public SendMessageBatchRequest getSendMessageBatchArg() {
        return sendMessageBatchArg;
    }

    public MockSQSAsyncClient addBatchResponseError(BatchResultErrorEntry entry) {
        this.batchResponseErrors.put(entry.id(), entry);
        return this;
    }

    public DeleteMessageBatchRequest getDeleteMessageBatchArg() {
        return deleteMessageBatchArg;
    }

    public CreateQueueRequest getCreateQueueArg() {
        return createQueueArg;
    }

    public GetQueueUrlRequest getGetQueueUrlArg() {
        return getQueueUrlArg;
    }

    public SendMessageRequest getSendMessageArg() {
        return sendMessageArg;
    }

    public MockSQSAsyncClient setCreateQueueResponse(CompletableFuture<CreateQueueResponse> createQueueResponse) {
        this.createQueueResponse = createQueueResponse;
        return this;
    }

    public MockSQSAsyncClient setGetQueueUrlResponse(CompletableFuture<GetQueueUrlResponse> getQueueUrlResponse) {
        this.getQueueUrlResponse = getQueueUrlResponse;
        return this;
    }

    @Override
    public CompletableFuture<ReceiveMessageResponse> receiveMessage(ReceiveMessageRequest receiveMessageRequest) {
        this.receiveMessageRequestArg = receiveMessageRequest;
        return this.receiveMessageResponse;
    }

    public MockSQSAsyncClient setSendMessageResponse(CompletableFuture<SendMessageResponse> sendMessageResponse) {
        this.sendMessageResponse = sendMessageResponse;
        return this;
    }

    @Override
    public String serviceName() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public CompletableFuture<CreateQueueResponse> createQueue(CreateQueueRequest createQueueRequest) {
        this.createQueueArg = createQueueRequest;
        return this.createQueueResponse;
    }

    @Override
    public CompletableFuture<GetQueueUrlResponse> getQueueUrl(GetQueueUrlRequest getQueueUrlRequest) {
        this.getQueueUrlArg = getQueueUrlRequest;
        return this.getQueueUrlResponse;
    }

    @Override
    public CompletableFuture<SendMessageResponse> sendMessage(SendMessageRequest sendMessageRequest) {
        this.sendMessageArg = sendMessageRequest;
        return this.sendMessageResponse;
    }

    @Override
    public CompletableFuture<SendMessageBatchResponse> sendMessageBatch(SendMessageBatchRequest sendMessageBatchRequest) {
        this.sendMessageBatchArg = sendMessageBatchRequest;


        return batchCallException.map(e -> CompletableFuture.<SendMessageBatchResponse>failedFuture(e))
                .orElse(CompletableFuture.completedFuture(SendMessageBatchResponse.builder()
                        .successful(sendMessageBatchRequest.entries().stream()
                                .map(SendMessageBatchRequestEntry::id)
                                .filter(((Predicate<? super String>) this.batchResponseErrors::containsKey).negate())
                                .map(id -> SendMessageBatchResultEntry.builder().id(id)
                                        .build())
                                .collect(Collectors.toList())).failed(sendMessageBatchRequest.entries().stream()
                                .map(SendMessageBatchRequestEntry::id)
                                .filter(batchResponseErrors::containsKey)
                                .map(batchResponseErrors::get)
                                .collect(Collectors.toList()))
                        .build()));
    }


    @Override
    public CompletableFuture<DeleteMessageBatchResponse> deleteMessageBatch(DeleteMessageBatchRequest deleteMessageBatchRequest) {
        this.deleteMessageBatchArg = deleteMessageBatchRequest;
        return batchCallException.map(e -> CompletableFuture.<DeleteMessageBatchResponse>failedFuture(e))
                .orElse(CompletableFuture.completedFuture(DeleteMessageBatchResponse.builder()
                        .successful(deleteMessageBatchRequest.entries().stream()
                                .map(DeleteMessageBatchRequestEntry::id)
                                .filter(((Predicate<? super String>) this.batchResponseErrors::containsKey).negate())
                                .map(id -> DeleteMessageBatchResultEntry.builder().id(id).build())
                                .collect(Collectors.toList())).failed(deleteMessageBatchRequest.entries().stream()
                                .map(DeleteMessageBatchRequestEntry::id)
                                .filter(this.batchResponseErrors::containsKey)
                                .map(batchResponseErrors::get)
                                .collect(Collectors.toList()))
                        .build()));
    }
}