package io.github.kn.flow.aws.sqs;

import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder;

import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import io.github.kn.flow.ByteBufferToTextLineCompletionSubscriberAdapter;
import io.github.kn.flow.CompletionStageItemProcessor;
import io.github.kn.flow.CompletionSubscriber;
import io.github.kn.flow.CompletionSubscriberImpl;

/**
 * Builder of {@link CompletionSubscriber} which does the following :-
 * <br>
 * <ol>
 * <li> Parse the incoming bytes ({@link List} of {@link ByteBuffer}) as text lines (as per
 * {@link java.net.http.HttpResponse.BodySubscribers#ofLines(Charset)}).
 * <li> Sends each line as an individual SQS messages.
 * </ol>
 *
 * @see ByteBufferToTextLineCompletionSubscriberAdapter
 * @see SQSPublishingItemProcessor
 */
public class SQSMessagePublishingSubscriberBuilder {

    public static final String DEFAULT_QUEUE_NAME = "textline-publisher";

    private volatile Optional<HttpClient> javaClient = Optional.empty();
    private volatile String qName = DEFAULT_QUEUE_NAME;
    private volatile Executor executor = ForkJoinPool.commonPool();
    private int maxConcurrency = CompletionSubscriberImpl.MAX_PROCESSING_CONCURRENCY;
    private int backPressureChunkSize = CompletionSubscriberImpl.BACK_PRESSURE_CHUNK_SIZE;
    private volatile String messageSeparator = null;
    private volatile Charset charset = StandardCharsets.UTF_8;
    private volatile SqsAsyncClientBuilder sqsAsyncClientBuilder = SqsAsyncClient.builder();


    public SQSMessagePublishingSubscriberBuilder maxConcurrency(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
        return this;
    }

    public SQSMessagePublishingSubscriberBuilder backPressureChunkSize(int backPressureChunkSize) {
        this.backPressureChunkSize = backPressureChunkSize;
        return this;
    }

    public SQSMessagePublishingSubscriberBuilder executor(final Executor exec) {
        this.executor = exec;
        return this;
    }

    public SQSMessagePublishingSubscriberBuilder client(final HttpClient client) {
        this.javaClient = Optional.of(client);
        return this;
    }

    public SQSMessagePublishingSubscriberBuilder charset(final Charset cSet) {
        this.charset = cSet;
        return this;
    }

    public SQSMessagePublishingSubscriberBuilder messageSeparator(final String separator) {
        this.messageSeparator = separator;
        return this;
    }

    public SQSMessagePublishingSubscriberBuilder queueName(final String name) {
        this.qName = name;
        return this;
    }

    public SQSMessagePublishingSubscriberBuilder sqsClientBuilder(final SqsAsyncClientBuilder builder) {
        this.sqsAsyncClientBuilder = builder;
        return this;
    }

    public CompletionSubscriber<List<ByteBuffer>> build() {
        return ByteBufferToTextLineCompletionSubscriberAdapter
                .adapt(CompletionSubscriberImpl
                        .wrap(CompletionStageItemProcessor.<String>completedProcessor()
                                .apply(Collections::singleton)
                                .apply(Collection::stream)
                                .compose(new SQSPublishingItemProcessor(sqsAsyncClientBuilder, qName, executor,
                                        javaClient.orElseGet(() -> HttpClient.newBuilder()
                                                .version(HttpClient.Version.HTTP_1_1)
                                                .followRedirects(HttpClient.Redirect.NORMAL)
                                                .connectTimeout(Duration.ofSeconds(200l))
                                                .executor(executor)
                                                .build())))
                                .apply(r -> r.findFirst()
                                        .filter(SQSPublishingItemProcessor.ResponseEntry::isSuccess)
                                        .orElseThrow(IllegalArgumentException::new)
                                ).apply(r -> null), maxConcurrency, backPressureChunkSize), charset, messageSeparator);
    }
}
