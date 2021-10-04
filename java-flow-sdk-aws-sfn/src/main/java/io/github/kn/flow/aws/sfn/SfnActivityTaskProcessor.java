package io.github.kn.flow.aws.sfn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.github.kn.flow.CompletionStageItemProcessor;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.services.sfn.SfnAsyncClient;
import software.amazon.awssdk.services.sfn.model.GetActivityTaskResponse;
import software.amazon.awssdk.services.sfn.model.SendTaskFailureRequest;
import software.amazon.awssdk.services.sfn.model.SendTaskSuccessRequest;
import software.amazon.awssdk.services.sfn.model.SendTaskSuccessResponse;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * A {@link CompletionStageItemProcessor} processing AWS Step functions activity tasks.
 * It transforms each activity task (JSON) input into an input object of type {@code T}
 * and invokes the wrapped {@link CompletionStageItemProcessor}. Once the {@link CompletionStageItemProcessor}
 * completes processing, the processing output of type {@code R} is transformed to the AWS-Sfn activity
 * (JSON) output.
 */
public class SfnActivityTaskProcessor<T, R> extends AbstractSfnProcessor<GetActivityTaskResponse,
        Void> {
    private static final Logger LOG = LogManager.getLogger("SfnActivityTaskProcessor");
    private final CompletionStageItemProcessor<T, R> delegateItemProcessor;

    private final Class<T> inputClass;

    private SfnActivityTaskProcessor(final CompletionStageItemProcessor<T, R> httpRequestItemProcessor,
                                     final SfnAsyncClient client, final Class<T> clazz) {
        super(client);
        this.inputClass = clazz;
        this.delegateItemProcessor = httpRequestItemProcessor;
    }

    public static SfnActivityTaskProcessor.Builder<SimpleJsonInputOutput, SimpleJsonInputOutput> builder(final CompletionStageItemProcessor<String,
            String> processor) {
        return builder(new CompletionStageItemProcessor<>() {
            @Override
            public CompletionStage<Void> prepare() {
                return processor.prepare();
            }

            @Override
            public CompletionStage<SimpleJsonInputOutput> onNext(final SimpleJsonInputOutput item) {
                SimpleJsonInputOutput io = new SimpleJsonInputOutput().setInput(item.getInput());
                return processor.onNext(String.valueOf(item.getInput()))
                        .thenApply(io::setOutput);
            }
        }, SimpleJsonInputOutput.class);
    }

    public static <T, R> SfnActivityTaskProcessor.Builder<T, R> builder(final CompletionStageItemProcessor<T,
            R> processor, Class<T> clazz) {
        return new Builder<>(processor, clazz);
    }

    @Override
    public CompletionStage<Void> onNext(final GetActivityTaskResponse response) {
        LOG.trace(" onNext invoked {}", response);
        SfnTaskFinisher finisher = new SfnTaskFinisher(response);
        return Optional.ofNullable(response.taskToken()).map(token ->
                processInput(response.input()).thenCompose(
                        finisher::finish
                ).thenAccept(a -> {
                }).exceptionally(finisher::handleException)).orElse(CompletableFuture.allOf());
    }

    private CompletionStage<R> processInput(final String input) {
        try {
            return delegateItemProcessor.onNext(getObject(input, inputClass));
        } catch (Throwable t) {
            return CompletableFuture.failedStage(new IllegalArgumentException(t.getMessage(), t));
        }
    }

    public static class Builder<T, R> {
        private final Class<T> inputClass;
        private final CompletionStageItemProcessor<T, R> inputProcessor;
        private volatile Optional<SfnAsyncClient> sfnAsyncClient = Optional.empty();
        private volatile Executor executor = ForkJoinPool.commonPool();

        public Builder(final CompletionStageItemProcessor<T, R> processor,
                       final Class<T> clazz) {
            this.inputProcessor = processor;
            inputClass = clazz;
        }

        public Builder<T, R> client(final SfnAsyncClient client) {
            this.sfnAsyncClient = Optional.of(client);
            return this;
        }

        public Builder<T, R> executor(final Executor exec) {
            this.executor = exec;
            return this;
        }

        public SfnActivityTaskProcessor<T, R> build() {
            return new SfnActivityTaskProcessor<>(inputProcessor, sfnAsyncClient
                    .orElseGet(() -> SfnAsyncClient.builder()
                            .asyncConfiguration(ClientAsyncConfiguration.builder()
                                    .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, executor)
                                    .build())
                            .build()), inputClass);
        }
    }

    private final class SfnTaskFinisher {
        static final int MAX_ERROR_CHARS = 256;
        private final GetActivityTaskResponse response;

        private SfnTaskFinisher(final GetActivityTaskResponse response) {
            this.response = response;
        }

        private Void handleException(final Throwable t) {
            String errorStr = String.valueOf(t.getMessage());
            LOG.error("Exception processing task :{} Error:{} ", response, errorStr);
            getSfnAsyncClient()
                    .sendTaskFailure(SendTaskFailureRequest.builder()
                            .error(errorStr.substring(0, Math.min(MAX_ERROR_CHARS, errorStr.length())))
                            .cause(Arrays.toString(t.getStackTrace()))
                            .taskToken(response.taskToken())
                            .build()).whenComplete((r, e) -> Optional.ofNullable(e)
                    .ifPresentOrElse(exception -> LOG.error("Exception failing task: " + response, exception), () -> LOG
                            .info("Failed task {} with response {}", response, r)));
            throw t instanceof IllegalArgumentException ? (IllegalArgumentException) t :
                    new IllegalArgumentException(t);
        }

        private CompletableFuture<SendTaskSuccessResponse> finish(final R processingResponse) {
            LOG.debug("Marking task {} finished with result {}", response, processingResponse);
            CompletableFuture<SendTaskSuccessResponse> returnedCompletable;
            returnedCompletable = getSfnAsyncClient()
                    .sendTaskSuccess(SendTaskSuccessRequest.builder()
                            .output(getJsonizedOutput(processingResponse))
                            .taskToken(response.taskToken())
                            .build())
                    .whenComplete((a, t) -> Optional.ofNullable(t)
                            .ifPresent(this::handleException));
            return returnedCompletable;
        }
    }
}

