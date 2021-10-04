package io.github.kn.flow.aws.sqs;

import io.github.kn.flow.CompletionStageItemProcessor;
import io.github.kn.flow.aws.JavaNativeSdkHttpClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder;
import software.amazon.awssdk.services.sqs.model.*;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implements a generic AWS-SQS message publisher, where each published message is harvested as
 * a {@link CompletionStageItemProcessor} of upstream published items ({@code onNext}).
 */
public class SQSPublishingItemProcessor implements CompletionStageItemProcessor<Stream<String>,
        Stream<SQSPublishingItemProcessor.ResponseEntry>> {

    private static final Logger LOG = LogManager.getLogger("SQSPublishingItemProcessor");
    private final String queueName;
    private final SqsAsyncClient sqsClient;
    private volatile String queueURL;

    /**
     * @param client {@link SqsAsyncClient}
     * @param qName  Name of the AWS-SQS queue to publish on.
     */
    public SQSPublishingItemProcessor(final SqsAsyncClient client, final String qName) {
        sqsClient = client;
        this.queueName = qName;
    }

    public SQSPublishingItemProcessor(final String qName) {
        this(SqsAsyncClient.builder()
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.defaultRetryPolicy())
                        .build())
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                        .maxConcurrency(10000).maxPendingConnectionAcquires(100000)
                        .connectionTimeout(Duration.ofSeconds(60l))
                        .connectionAcquisitionTimeout(Duration.ofSeconds(300l))).build(), qName);
    }


    public SQSPublishingItemProcessor(final SqsAsyncClientBuilder clientBuilder, final String qName,
                                      final Executor executor, final HttpClient javaHttpClient) {
        this(clientBuilder.asyncConfiguration(ClientAsyncConfiguration.builder()
                .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, executor)
                .build())
                .httpClient(new JavaNativeSdkHttpClient(javaHttpClient)).build(), qName);
    }

    @Override
    public CompletionStage<Void> prepare() {
        return sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName)
                .build()).thenApply(r -> r.queueUrl())
                .exceptionally(this::handleException)
                .thenCompose(r -> Optional.ofNullable(r).map(CompletableFuture::completedFuture)
                        .orElseGet(this::createQueue)).thenAccept(u -> this.queueURL = u);
    }


    private Throwable getUnderlyingThrowable(final Throwable t) {
        return t instanceof ExecutionException || t instanceof CompletionException ? t.getCause() : t;
    }

    private String handleException(final Throwable throwable) {
        if (getUnderlyingThrowable(throwable) instanceof QueueDoesNotExistException) {
            LOG.warn("Queue with name {} does not already exist ... creating:", queueName);
        } else if (getUnderlyingThrowable(throwable) instanceof InvalidMessageContentsException
        ) {
            throw new IllegalArgumentException(getUnderlyingThrowable(throwable));
        } else {
            throw new CompletionException(getUnderlyingThrowable(throwable));
        }
        return null;
    }

    private CompletableFuture<String> createQueue() {
        return sqsClient.createQueue(CreateQueueRequest.builder().queueName(queueName).build())
                .thenApply(r -> r.queueUrl())
                .exceptionally(this::handleException);
    }

    private Stream<ResponseEntry> from(final SendMessageBatchResponse response) {
        LinkedList<Map.Entry<String, ResponseEntry>> responseList = new LinkedList<>();
        response.failed().stream().map(e -> Map.entry(e.id(), (ResponseEntry) new ErrorResponse(e)))
                .forEach(responseList::add);
        response.successful().stream()
                .map(e -> Map.entry(e.id(), (ResponseEntry) new SuccessResponse(e))).forEach(responseList::add);
        LOG.debug("SendMessageBatchResponse for queue :{} is :{}", queueName, responseList);
        return responseList.stream().sorted(Comparator.comparing(Map.Entry::getKey)).map(Map.Entry::getValue);
    }

    @Override
    public CompletionStage<Stream<ResponseEntry>> onNext(final Stream<String> items) {
        LOG.trace("onNext invoked for queueName {} and items {}", queueName, items);
        List<SendMessageBatchRequestEntry> requestEntries = getRequestEntries(items);
        if (!requestEntries.isEmpty()) {
            return sqsClient
                    .sendMessageBatch(SendMessageBatchRequest.builder().queueUrl(this.queueURL)
                            .entries(requestEntries)
                            .build())
                    .thenApply(this::from)
                    .exceptionally(t -> {
                                LOG.error("Exception publishing message :{} on queue:{}", items, queueName);
                                handleException(t);
                                return null;
                            }
                    );
        }
        return CompletableFuture.completedStage(Collections.<ResponseEntry>emptyList().stream());
    }

    private List<SendMessageBatchRequestEntry> getRequestEntries(Stream<String> items) {
        AtomicInteger id = new AtomicInteger(0);
        return items.map(s -> SendMessageBatchRequestEntry.builder().messageBody(s)
                .id(String.valueOf(id.getAndIncrement())).build()).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return queueName;
    }

    public interface ResponseEntry {
        default boolean isSuccess() {
            return true;
        }
        String message();
    }

    private static class ErrorResponse implements ResponseEntry {
        private final BatchResultErrorEntry entry;

        private ErrorResponse(BatchResultErrorEntry entry) {
            this.entry = entry;
        }

        @Override
        public boolean isSuccess() {
            return false;
        }

        @Override
        public String message() {
            return toString();
        }

        @Override
        public String toString() {
            return entry.toString();
        }
    }

    private static class SuccessResponse implements ResponseEntry {
        private final SendMessageBatchResultEntry entry;

        private SuccessResponse(final SendMessageBatchResultEntry entry) {
            this.entry = entry;
        }

        @Override
        public String message() {
            return toString();
        }

        @Override
        public String toString() {
            return entry.toString();
        }
    }
}