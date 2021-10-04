package io.github.kn.flow;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 *
 */
class CompletionStageItemProcessorTest {

    private MyCompletionStageItemProcessor<String, String> myCompletionStageItemProcessor;

    @BeforeEach
    void setUp() {
        myCompletionStageItemProcessor = new MyCompletionStageItemProcessor<String, String>()
                .setOnNextResult(CompletableFuture.completedStage("output"));
    }

    @Test
    void aggregateProcessorSuccess() throws Exception {
        Assertions.assertEquals("outputoutputoutput", CompletionStageItemProcessor
                .aggregateProcessor(myCompletionStageItemProcessor)
                .onNext(Optionals.create(List.of("1", "2", "3"))).toCompletableFuture().get().toString());
    }

    @Test
    void aggregateProcessorFailure() throws Exception {
        myCompletionStageItemProcessor = new MyCompletionStageItemProcessor<String, String>()
                .setOnNextResult(CompletableFuture.failedFuture(new IllegalArgumentException()));
        Assertions.assertEquals("", CompletionStageItemProcessor
                .aggregateProcessor(myCompletionStageItemProcessor)
                .onNext(Optionals.wrap(List.of(Optional.of("1")))).toCompletableFuture().get()
                .toString());
    }

    @Test
    void apply() throws Exception {
        Assertions.assertEquals("outputmodified", myCompletionStageItemProcessor
                .apply(s -> s + "modified").onNext(null).toCompletableFuture().get());
    }

    @Test
    void applyBiFunction() throws Exception {
        Assertions.assertEquals("inputoutputmodified", myCompletionStageItemProcessor
                .apply((i, o) -> i + o + "modified").onNext("input").toCompletableFuture().get());
    }

    @Test
    void composeAsync() throws Exception {
        Assertions.assertEquals("firstStageResult", new MyCompletionStageItemProcessor<Integer, String>()
                .setOnNextResult(CompletableFuture.completedStage("firstStageResult"))
                .composeAsync(myCompletionStageItemProcessor).onNext(1).toCompletableFuture().get());
        Assertions.assertEquals(List.of("firstStageResult"), myCompletionStageItemProcessor.getOnNextInput());
    }

    @Test
    void composeAsyncInvokesComposedPrepare() {
        new MyCompletionStageItemProcessor<Integer, String>()
                .composeAsync(myCompletionStageItemProcessor).prepare();
        Assertions.assertEquals(1, myCompletionStageItemProcessor.getTimesPrepareInvoked());
    }

    @Test
    void composeAsyncOptionallyPredicateTrue() throws Exception {
        Assertions.assertEquals("firstStageResult", new MyCompletionStageItemProcessor<Integer, String>()
                .setOnNextResult(CompletableFuture.completedStage("firstStageResult"))
                .composeAsyncOptionally(s -> true, myCompletionStageItemProcessor).onNext(1).toCompletableFuture()
                .get());
        Assertions.assertEquals(List.of("firstStageResult"), myCompletionStageItemProcessor.getOnNextInput());
    }

    @Test
    void composeAsyncOptionallyPredicateFalse() {
        new MyCompletionStageItemProcessor<Integer, String>()
                .setOnNextResult(CompletableFuture.completedStage("firstStageResult"))
                .composeAsyncOptionally(s -> false, myCompletionStageItemProcessor).onNext(1);
        Assertions.assertEquals(0, myCompletionStageItemProcessor.getOnNextInput().size());
    }

    @Test
    void composeAsyncOptionallyInvokesComposedPrepare() {
        new MyCompletionStageItemProcessor<Integer, String>()
                .composeAsyncOptionally(s -> false, myCompletionStageItemProcessor).prepare();
        Assertions.assertEquals(1, myCompletionStageItemProcessor.getTimesPrepareInvoked());
    }

    @Test
    void compose() throws Exception {
        Assertions.assertEquals("output", new MyCompletionStageItemProcessor<Integer, String>()
                .setOnNextResult(CompletableFuture.completedStage("firstStageResult"))
                .compose(myCompletionStageItemProcessor).onNext(1).toCompletableFuture().get());
        Assertions.assertEquals(List.of("firstStageResult"), myCompletionStageItemProcessor.getOnNextInput());
    }


    @Test
    void composeInvokesComposedPrepare() {
        new MyCompletionStageItemProcessor<Integer, String>()
                .compose(myCompletionStageItemProcessor).prepare();
        Assertions.assertEquals(1, myCompletionStageItemProcessor.getTimesPrepareInvoked());
    }

    @Test
    void composeOptionally() throws Exception {
        Assertions.assertEquals("output", new MyCompletionStageItemProcessor<Integer, String>()
                .setOnNextResult(CompletableFuture.completedStage("firstStageResult"))
                .composeOptionally(s -> true, myCompletionStageItemProcessor).onNext(1).toCompletableFuture().get()
                .get());
        Assertions.assertEquals(List.of("firstStageResult"), myCompletionStageItemProcessor.getOnNextInput());
    }

    @Test
    void composeOptionallyPredicateFalse() throws Exception {
        Assertions.assertTrue(new MyCompletionStageItemProcessor<Integer, String>()
                .setOnNextResult(CompletableFuture.completedStage("firstStageResult"))
                .composeOptionally(s -> false, myCompletionStageItemProcessor).onNext(1).toCompletableFuture().get()
                .isEmpty());
        Assertions.assertEquals(0, myCompletionStageItemProcessor.getOnNextInput().size());
    }

    @Test
    void composeOptionallyInvokesComposedPrepare() {
        new MyCompletionStageItemProcessor<Integer, String>()
                .composeOptionally(s -> false, myCompletionStageItemProcessor).prepare();
        Assertions.assertEquals(1, myCompletionStageItemProcessor.getTimesPrepareInvoked());
    }

    private class MyCompletionStageItemProcessor<T, R> implements CompletionStageItemProcessor<T, R> {
        private CompletionStage<R> onNextResult;
        private List<T> onNextInputs = new ArrayList<>();
        private int timesPrepareInvoked;

        public int getTimesPrepareInvoked() {
            return timesPrepareInvoked;
        }

        public List<T> getOnNextInput() {
            return onNextInputs;
        }

        @Override
        public CompletionStage<Void> prepare() {
            timesPrepareInvoked++;
            return CompletableFuture.completedStage(null);
        }

        public MyCompletionStageItemProcessor<T, R> setOnNextResult(CompletionStage<R> onNextResult) {
            this.onNextResult = onNextResult;
            return this;
        }

        @Override
        public CompletionStage<R> onNext(T item) {
            this.onNextInputs.add(item);
            return this.onNextResult;
        }
    }
}