package io.github.kn.flow.aws.sfn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.github.kn.flow.AbstractPollingPublisher;
import software.amazon.awssdk.services.sfn.SfnAsyncClient;
import software.amazon.awssdk.services.sfn.model.GetActivityTaskRequest;
import software.amazon.awssdk.services.sfn.model.GetActivityTaskResponse;

import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * An {@link java.util.concurrent.Flow.Publisher} of AWS-Sfn activity tasks.
 * <br>
 * This implementation continuously polls AWS-Step function activity ARNs for tasks on a configured {@link Executor}.
 * Each polled task {@link GetActivityTaskResponse} is then published to the registered subscribers for further
 * processing.
 */
public class SfnActivityTaskPublisher extends AbstractPollingPublisher<GetActivityTaskResponse> {

    private static final Logger LOG = LogManager.getLogger("SfnActivityTaskPublisher");


    SfnActivityTaskPublisher(final Supplier<Runnable> runnableSupplier, final Executor pollExec,
                             final int maxPollingThreads,
                             final Flow.Publisher<GetActivityTaskResponse> publisher) {
        super(runnableSupplier, pollExec, maxPollingThreads, publisher);
    }

    public static BuilderImpl builder(final String arn) {
        return new SfnActivityTaskPublisher.BuilderImpl(arn);
    }

    public static class BuilderImpl extends Builder<BuilderImpl> {
        private final String arnStr;
        private volatile Optional<SfnAsyncClient> sfnAsyncClient = Optional.empty();

        BuilderImpl(final String arnStr) {
            this.arnStr = arnStr;
        }

        public BuilderImpl client(final SfnAsyncClient client) {
            this.sfnAsyncClient = Optional.of(client);
            return getThis();
        }

        public SfnActivityTaskPublisher build() {
            SubmissionPublisher<GetActivityTaskResponse> publisher = getPublisher();
            return new SfnActivityTaskPublisher(() -> new SfnPoller(sfnAsyncClient
                    .orElseGet(() -> SfnAsyncClient.builder()
                            .asyncConfiguration(getDefaultAsyncConfig())
                            .httpClientBuilder(getLongPollingHttpClientBuilder())
                            .build()), arnStr, getMaxPollsPerThread(), publisher), Executors
                    .newCachedThreadPool(), getMaxPollingThreads(), publisher);
        }

        @Override
        public BuilderImpl getThis() {
            return this;
        }
    }

    static final class SfnPoller extends PollingRunnable<GetActivityTaskResponse> {
        private final String activityArn;
        private final SfnAsyncClient sfnAsyncClient;

        SfnPoller(final SfnAsyncClient client, final String arn, final int maxPollsPerThread,
                  final SubmissionPublisher<GetActivityTaskResponse> pub) {
            this(client, arn, maxPollsPerThread, pub, new LinkedBlockingQueue<>());
        }

        SfnPoller(final SfnAsyncClient client, final String arn, final int maxPollsPerThread,
                  final SubmissionPublisher<GetActivityTaskResponse> pub, final BlockingQueue<GetActivityTaskResponse>
                          queue) {
            super(maxPollsPerThread, queue, pub);
            this.sfnAsyncClient = client;
            this.activityArn = arn;
        }


        @Override
        protected void init() {

        }


        @Override
        protected CompletionStage<Optional<GetActivityTaskResponse>> pollImpl() {
            LOG.trace("Polling {}", activityArn);
            return sfnAsyncClient.getActivityTask(GetActivityTaskRequest.builder().activityArn(activityArn)
                    .workerName(this.getClass().getName()).build())
                    .thenApply(response -> response != null && response.taskToken() != null && !response.taskToken()
                            .isEmpty() ? Optional
                            .of(response) : Optional.empty());
        }
    }
}
