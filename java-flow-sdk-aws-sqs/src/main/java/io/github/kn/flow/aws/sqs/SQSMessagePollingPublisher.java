package io.github.kn.flow.aws.sqs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.github.kn.flow.AbstractPollingPublisher;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * An {@link java.util.concurrent.Flow.Publisher} of AWS-SQS ReceiveMessageResponses.
 * <br>
 * This implementation continuously polls AWS-SQS for messages on a configured {@link Executor}.
 * Each polled item {@link ReceiveMessageResponse} is then published to the registered subscribers for further
 * processing.
 */
public class SQSMessagePollingPublisher extends AbstractPollingPublisher<ReceiveMessageResponse> {
    private static final Logger LOG = LogManager.getLogger("SQSMessagePollingPublisher");

    SQSMessagePollingPublisher(final Supplier<Runnable> runnableSupplier, final Executor pollExec,
                               final int maxPollingThreads,
                               final Flow.Publisher<ReceiveMessageResponse> publisher) {
        super(runnableSupplier, pollExec, maxPollingThreads, publisher);
    }


    public static SQSMessagePollingPublisher.BuilderImpl builder(final String name) {
        return new SQSMessagePollingPublisher.BuilderImpl(name);
    }

    public static class BuilderImpl extends Builder<BuilderImpl> {

        private final String qName;
        private Optional<SqsAsyncClient> sqsAsyncClient = Optional.empty();
        private volatile int pollBatchSize = 10;

        private BuilderImpl(final String name) {
            this.qName = name;
        }

        public BuilderImpl setPollBatchSize(final int pollBatchSize) {
            this.pollBatchSize = pollBatchSize;
            return this;
        }

        @Override
        public BuilderImpl getThis() {
            return this;
        }

        public BuilderImpl client(final SqsAsyncClient client) {
            this.sqsAsyncClient = Optional.of(client);
            return this;
        }

        public SQSMessagePollingPublisher build() {
            SubmissionPublisher<ReceiveMessageResponse> publisher = getPublisher();
            return new SQSMessagePollingPublisher(() -> new SQSPoller(sqsAsyncClient
                    .orElseGet(() -> SqsAsyncClient.builder()
                            .asyncConfiguration(getDefaultAsyncConfig())
                            .httpClientBuilder(getLongPollingHttpClientBuilder())
                            .build()), qName, getMaxPollsPerThread(), pollBatchSize, publisher), Executors
                    .newCachedThreadPool(), getMaxPollingThreads(), publisher);
        }
    }

    static final class SQSPoller extends PollingRunnable<ReceiveMessageResponse> {
        private final String qName;
        private final SqsAsyncClient sqsAsyncClient;
        private final Integer pollBatchSize;
        private volatile String queueURL;

        SQSPoller(final SqsAsyncClient client, final String name, final int maxPollsPerThread, final int batchSize,
                  final SubmissionPublisher<ReceiveMessageResponse> pub) {
            this(client, name, maxPollsPerThread, batchSize, pub, new LinkedBlockingQueue<>());
        }

        SQSPoller(final SqsAsyncClient client, final String name, final int maxPollsPerThread, final int batchSize,
                  final SubmissionPublisher<ReceiveMessageResponse> pub, final BlockingQueue<ReceiveMessageResponse>
                          queue) {
            super(maxPollsPerThread, queue, pub);
            this.sqsAsyncClient = client;
            this.pollBatchSize = batchSize;
            this.qName = name;
        }

        @Override
        protected void init() {
            LOG.info("Querying queue URL for {}", qName);
            try {
                this.queueURL = this.sqsAsyncClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(qName).build())
                        .get()
                        .queueUrl();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }


        @Override
        protected CompletionStage<Optional<ReceiveMessageResponse>> pollImpl() {
            LOG.trace("Polling queue {}", queueURL);
            return sqsAsyncClient
                    .receiveMessage(ReceiveMessageRequest.builder().waitTimeSeconds(20)
                            .maxNumberOfMessages(pollBatchSize)
                            .queueUrl(queueURL).build())
                    .thenApply(response -> response.messages() != null && response.messages().size() > 0 ? Optional
                            .of(response) : Optional.empty());
        }
    }
}
