package io.github.kn.flow.util;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import io.github.kn.flow.CompletionStageItemProcessor;

/**
 *
 */
public class MockCompletionStageItemProcessor<T, R> implements CompletionStageItemProcessor<T, R> {

    private CompletionStage<R> onNextResponse;
    private T onNextItem;
    private int timesPrepareInvoked;
    private CompletionStage<Void> prepareCompletionStage;
    private Optional<RuntimeException> onNextException = Optional.empty();

    public MockCompletionStageItemProcessor<T, R> setOnNextException(RuntimeException onNextException) {
        this.onNextException = Optional.of(onNextException);
        return this;
    }

    public int getTimesPrepareInvoked() {
        return timesPrepareInvoked;
    }

    public MockCompletionStageItemProcessor<T, R> setPrepareCompletionStage(CompletionStage<Void> prepareCompletionStage) {
        this.prepareCompletionStage = prepareCompletionStage;
        return this;
    }

    public MockCompletionStageItemProcessor<T, R> setOnNextResponse(CompletionStage<R> onNextResponse) {
        this.onNextResponse = onNextResponse;
        return this;
    }

    public T getOnNextItem() {
        return onNextItem;
    }

    @Override
    public CompletionStage<Void> prepare() {
        this.timesPrepareInvoked++;
        return prepareCompletionStage;
    }

    @Override
    public CompletionStage<R> onNext(T item) {
        onNextException.ifPresent(e -> {
            throw e;
        });
        this.onNextItem = item;
        return this.onNextResponse;
    }
}