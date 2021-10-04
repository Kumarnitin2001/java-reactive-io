package io.github.kn.flow.aws.sqs;

import io.github.kn.flow.CompletionStageItemProcessor;
import io.github.kn.flow.Optionals;
import io.github.kn.flow.aws.JavaNativeSdkHttpClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder;
import software.amazon.awssdk.services.sqs.model.*;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * A {@link CompletionStageItemProcessor} processing AWS -SQS ReceiveMessageResponses.
 */
public class SQSReceivedMessageResponseProcessor implements CompletionStageItemProcessor<ReceiveMessageResponse, Void> {

    private static final Logger LOG = LogManager.getLogger("SQSReceivedMessageResponseProcessor");
    private final String queueName;
    private final SqsAsyncClient sqsClient;
    private final CompletionStageItemProcessor<Optionals<String>, Optionals<?>> messagesProcessor;
    private volatile String queueURL;

    /**
     * @param client {@link SqsAsyncClient}
     * @param qName  Name of the AWS-SQS queue to publish on.
     */
    public SQSReceivedMessageResponseProcessor(final SqsAsyncClient client, final String qName,
                                               final CompletionStageItemProcessor<Optionals<String>,
                                                       Optionals<?>> processor) {
        sqsClient = client;
        this.queueName = qName;
        this.messagesProcessor = processor;
    }

    public SQSReceivedMessageResponseProcessor(final String qName,
                                               final CompletionStageItemProcessor<Optionals<String>,
                                                       Optionals<?>> processor) {
        this(SqsAsyncClient.builder()
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.defaultRetryPolicy())
                        .build())
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                        .maxConcurrency(10000).maxPendingConnectionAcquires(100000)
                        .connectionTimeout(Duration.ofSeconds(60l))
                        .connectionAcquisitionTimeout(Duration.ofSeconds(300l))).build(), qName, processor);
    }

    public SQSReceivedMessageResponseProcessor(final SqsAsyncClientBuilder clientBuilder, final String qName,
                                               final HttpClient javaHttpClient
            , final CompletionStageItemProcessor<Optionals<String>, Optionals<?>> processor) {
        this(clientBuilder
                .httpClient(new JavaNativeSdkHttpClient(javaHttpClient)).build(), qName, processor);
    }

    @Override
    public CompletionStage<Void> prepare() {
        return sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName)
                .build()).thenApply(r -> r.queueUrl())
                .thenAccept(u -> this.queueURL = u).thenCompose(a -> messagesProcessor.prepare());
    }

    private CompletionStage<?> deleteMessages(final Optionals<Message> messages) {
        AtomicInteger id = new AtomicInteger();
        Collection<DeleteMessageBatchRequestEntry> entries = messages
                .map(m -> DeleteMessageBatchRequestEntry.builder().id(String.valueOf(id.incrementAndGet()))
                        .receiptHandle(m.receiptHandle()).build())
                .stream().collect(Collectors.toSet());
        return entries.isEmpty() ? CompletableFuture.allOf() : sqsClient
                .deleteMessageBatch(DeleteMessageBatchRequest.builder().queueUrl(queueURL).entries(entries)
                        .build());
    }

    @Override
    public CompletionStage<Void> onNext(final ReceiveMessageResponse response) {
        CompletionStage<Void> returnedCompletable = CompletableFuture.allOf();
        List<Message> inputMessages = response.messages().stream().filter(Objects::nonNull)
                .filter(m -> Objects.nonNull(m.body())).filter(m -> !m.body().isBlank())
                .collect(Collectors.toList());
        LOG.trace("onNext invoked for queue {} with Messages {}", queueName, inputMessages);
        if (!inputMessages.isEmpty()) {
            Optionals<Message> messages = Optionals
                    .create(inputMessages);
            try {
                return this.messagesProcessor.onNext(messages.map(Message::body))
                        .thenAccept(s -> deleteMessages(messages.mergeEmpties(s)))
                        .whenComplete((r, e) -> Optional.ofNullable(e)
                                .ifPresentOrElse(exception -> LOG
                                        .error("Exception processing queue: " + queueName + " messages: " + messages,
                                                exception), () -> LOG
                                        .debug("Completed processing queue {} messages {}", queueName, messages)))
                        .exceptionally(t -> {
                            throw new IllegalArgumentException(t);
                        });
            } catch (Throwable t) {
                returnedCompletable = CompletableFuture
                        .failedStage(new IllegalArgumentException(t));
            }
        }
        return returnedCompletable;
    }
}
