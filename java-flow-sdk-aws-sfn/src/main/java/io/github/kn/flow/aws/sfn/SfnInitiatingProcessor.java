package io.github.kn.flow.aws.sfn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.github.kn.flow.CompletionStageItemProcessor;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.services.sfn.SfnAsyncClient;
import software.amazon.awssdk.services.sfn.model.ExecutionAlreadyExistsException;
import software.amazon.awssdk.services.sfn.model.StartExecutionRequest;
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * A {@link CompletionStageItemProcessor} starting AWS-Sfn activity.
 */
public class SfnInitiatingProcessor<T> extends AbstractSfnProcessor<T,
        StartExecutionResponse> {

    private static final Logger LOG = LogManager.getLogger("SfnInitiatingProcessor");
    private static final String EXECUTION_NAME_PREFIX = "AutoExecution-";
    private final String stateMachineARN;
    /**
     *
     * @param client
     * @param smARN
     */
    public SfnInitiatingProcessor(final SfnAsyncClient client, final String smARN) {
        super(client);
        this.stateMachineARN = smARN;
    }

    public static <T> SfnInitiatingProcessor.Builder<T> builder(final String smARN) {
        return new SfnInitiatingProcessor.Builder<>(smARN);
    }

    private CompletionStage<StartExecutionResponse> invokeSfn(final String input) {
        return getSfnAsyncClient().startExecution(StartExecutionRequest.builder().stateMachineArn(stateMachineARN)
                .name(EXECUTION_NAME_PREFIX + new Random().nextLong()).input(input).build());
    }

    @Override
    public CompletionStage<StartExecutionResponse> onNext(final T input) {
        String inputStr = getJsonizedInput(input);
        LOG.trace("onNext invoked for input {}", inputStr);
        return invokeSfn(inputStr)
                .handle((r, t) -> this.handleException(inputStr, r, t)).thenCompose(Function.identity());
    }

    private CompletionStage<StartExecutionResponse> handleException(final String item,
                                                                    final StartExecutionResponse response,
                                                                    final Throwable throwable) {
        return Optional.ofNullable(response).map(CompletableFuture::completedStage)
                .orElse(Optional.ofNullable(throwable)
                        .map(t -> t instanceof CompletionException || t instanceof ExecutionException ?
                                t.getCause() : t).filter(t -> t instanceof ExecutionAlreadyExistsException)
                        .map(t -> invokeSfn(item))
                        .orElseThrow(() -> new IllegalStateException("Exception invoking Sfn for input " + item,
                                throwable)));
    }

    public static class Builder<T> {
        private final String stateMachineARN;
        private volatile Optional<SfnAsyncClient> sfnAsyncClient = Optional.empty();
        private volatile Executor executor = ForkJoinPool.commonPool();

        public Builder(final String arn) {
            this.stateMachineARN = arn;
        }

        public SfnInitiatingProcessor.Builder<T> client(final SfnAsyncClient client) {
            this.sfnAsyncClient = Optional.of(client);
            return this;
        }

        public SfnInitiatingProcessor.Builder<T> executor(final Executor exec) {
            this.executor = exec;
            return this;
        }

        public SfnInitiatingProcessor<T> build() {
            return new SfnInitiatingProcessor<>(sfnAsyncClient
                    .orElseGet(() -> SfnAsyncClient.builder()
                            .asyncConfiguration(ClientAsyncConfiguration.builder()
                                    .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, executor)
                                    .build())
                            .build()), stateMachineARN);
        }
    }
}
