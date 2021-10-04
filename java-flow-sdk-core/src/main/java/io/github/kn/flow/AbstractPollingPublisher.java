package io.github.kn.flow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * A bunch of abstract classes to allow polling for elements of type T from respective source
 * repositories. The polled items are published using Java native {@link Flow} API framework.
 *
 * <p> Leverages Flow API back-pressure to moderate polling calls to source repository.
 * Allows for configuring basic polling parameters like number of polling threads,
 * maximum buffer capacity to buffer items before they are consumed.
 *
 * @see SubmissionPublisher
 * @see java.util.concurrent.Flow.Publisher
 * @see java.util.concurrent.Flow.Subscriber
 */
public abstract class AbstractPollingPublisher<T> implements Flow.Publisher<T> {
    private static final Logger LOG = LogManager.getLogger("AbstractPollingPublisher");
    private final Executor pollingExecutor;
    private final int maximumPollingThreads;
    private final Supplier<Runnable> taskRunnableSupplier;
    private final Flow.Publisher<T> publisherDelegate;

    protected AbstractPollingPublisher(final Supplier<Runnable> runnableSupplier,
                                       final Executor pollExec,
                                       final int maxPollingThreads,
                                       final Flow.Publisher<T> publisher) {
        this.taskRunnableSupplier = runnableSupplier;
        this.maximumPollingThreads = maxPollingThreads;
        this.pollingExecutor = pollExec;
        this.publisherDelegate = publisher;
    }


    @Override
    public void subscribe(final Flow.Subscriber<? super T> subscriber) {
        publisherDelegate.subscribe(subscriber);
        IntStream.range(0, maximumPollingThreads)
                .forEach(i -> pollingExecutor
                        .execute(this.taskRunnableSupplier.get()));
        LOG.info(" {} subscribed successfully {}", subscriber);
    }


    public abstract static class Builder<T extends Builder<T>> {
        private volatile int maxPollsPerThread = 100;
        private volatile int maxPollingThreads = 1;
        private volatile int bufferCapacity = 256;

        protected int getMaxPollsPerThread() {
            return maxPollsPerThread;
        }

        public T setMaxPollsPerThread(final int maxPollsPerThread) {
            this.maxPollsPerThread = maxPollsPerThread;
            return getThis();
        }

        protected int getMaxPollingThreads() {
            return maxPollingThreads;
        }

        public T setMaxPollingThreads(final int maxPollingThreads) {
            this.maxPollingThreads = maxPollingThreads;
            return getThis();
        }

        protected <S> SubmissionPublisher<S> getPublisher() {
            return new SubmissionPublisher<>(ForkJoinPool
                    .commonPool(), bufferCapacity);
        }

        protected ClientAsyncConfiguration getDefaultAsyncConfig() {
            return ClientAsyncConfiguration.builder()
                    .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                            ForkJoinPool.commonPool())
                    .build();
        }

        protected NettyNioAsyncHttpClient.Builder getLongPollingHttpClientBuilder() {
            return NettyNioAsyncHttpClient.builder()
                    .readTimeout(Duration.ofSeconds(65l))
                    .writeTimeout(Duration.ofSeconds(65l))
                    .connectionTimeout(Duration.ofSeconds(65l))
                    .connectionAcquisitionTimeout(Duration.ofSeconds(300l));
        }

        public T setBufferCapacity(final int capacity) {
            this.bufferCapacity = capacity;
            return getThis();
        }

        /**
         * The solution for the unchecked cast warning.
         */
        public abstract T getThis();
    }

    public static abstract class PollingRunnable<T> implements Runnable {

        private static final Logger LOG = LogManager.getLogger("PollingRunnable");
        private final BlockingQueue<CompletionStage<?>> pendingRequests;
        private final BlockingQueue<T> submittableTaskResponses;
        private final SubmissionPublisher<T> submissionPublisher;


        protected PollingRunnable(final int maxPollsPerThread,
                                  final BlockingQueue<T> submittableTaskResponses, final SubmissionPublisher<T> pub) {
            this.pendingRequests = new ArrayBlockingQueue<>(maxPollsPerThread);
            this.pendingRequests.add(CompletableFuture.allOf());
            this.submittableTaskResponses = submittableTaskResponses;
            this.submissionPublisher = pub;
        }


        protected abstract void init();

        protected abstract CompletionStage<Optional<T>> pollImpl();

        @Override
        public void run() {
            try {
                init();
            } catch (Throwable t) {
                LOG.fatal("Exception initializing ... exiting", t);
                throw t;
            }
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    poll();
                    submitTasks();
                } catch (Throwable t) {
                    LOG.error(" Exception in MAIN Loop ...", t);
                }
            }
            LOG.warn("MAIN loop has been exited due to interruption!!");
        }

        private void poll() throws InterruptedException {
            pendingRequests.put(pollImpl()
                    .thenAccept(o -> o.ifPresent(submittableTaskResponses::add))
                    .whenComplete((r, t) ->
                            pendingRequests.remove()).exceptionally(t ->
                    {
                        LOG.error("Completion exception polling task", t);
                        return null;
                    }));
        }

        public void submitTasks() {
            T task;
            while (Objects.nonNull(task = submittableTaskResponses.poll())) {
                LOG.trace("Submitting {}", task);
                submissionPublisher.submit(task);
            }
        }
    }
}
