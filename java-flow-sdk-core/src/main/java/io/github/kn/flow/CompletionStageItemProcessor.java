package io.github.kn.flow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An Async processor of items.
 */
@FunctionalInterface
public interface CompletionStageItemProcessor<T, R> {

    Logger LOG = LogManager.getLogger("CompletionStageItemProcessor");

    /**
     * Returns a new "Aggregate" CompletionStageItemProcessor wrapping the passed processor.
     * The passed processor processes every entry of the aggregate input, each of whose output is
     * aggregated back to form the result of the returned processor.
     *
     * @param processor input to result mapper
     * @param <V>       type of aggregate processing input
     * @param <W>       type of aggregate processing result
     * @return Completed {@link CompletionStageItemProcessor}
     * @see CompletableFuture#completedStage(Object)
     */
    static <V, W> CompletionStageItemProcessor<Optionals<V>, Optionals<W>> aggregateProcessor
    (final CompletionStageItemProcessor<V, W> processor) {
        return new CompletionStageItemProcessor<>() {
            @Override
            public CompletionStage<Void> prepare() {
                return processor.prepare();
            }

            private void logException(final CompletableFuture<W> future) {
                future.whenComplete((r, t) -> Optional.of(t)
                        .ifPresent(e -> LOG.error("aggregate processing exception", t)));
            }

            @Override
            public CompletionStage<Optionals<W>> onNext(final Optionals<V> items) {
                List<CompletableFuture<W>> futures = items.stream()
                        .map(processor::onNext)
                        .map(CompletionStage::toCompletableFuture).collect(Collectors.toList());
                CompletableFuture<Optionals<W>> result = new CompletableFuture<>();
                CompletableFuture.allOf(futures
                        .toArray(CompletableFuture[]::new))
                        .whenComplete((r, e) ->
                                result.complete(
                                        items.replace(
                                                futures.stream()
                                                        .peek(this::logException)
                                                        .map(s -> s.isDone() && !s.isCompletedExceptionally()
                                                                ? s.getNow(null)
                                                                : null).collect(Collectors.toList())
                                        )));
                return result;
            }
        };
    }

    /**
     * Returns a new CompletionStageItemProcessor which is already completed.
     *
     * @param <V> type of processing input and output
     * @return Completed {@link CompletionStageItemProcessor}
     * @see CompletableFuture#completedStage(Object)
     */
    static <V> CompletionStageItemProcessor<V, V> completedProcessor() {
        return CompletableFuture::completedStage;
    }

    /**
     * Invoked once before the processing begins. The {@code onNext} calls
     * are issues only when the returned completion stage completes successfully.
     *
     * @return {@link CompletionStage} marking future preparation completion.
     */
    default CompletionStage<Void> prepare() {
        return CompletableFuture.allOf();
    }

    /**
     * Invoked to process the next available item. Note that the multiple  calls
     * to onNext can be issues without waiting completion of previously returned stage(s).
     *
     * @param item reference to the next available processing item.
     * @return {@link CompletionStage} marking future completion of processing.
     */
    CompletionStage<R> onNext(final T item);

    /**
     * Returns a new CompletionStageItemProcessor that, when this processor completes normally, completes with this
     * processors's result as the argument to the supplied function.
     * See the CompletionStage documentation for rules covering exceptional completion.
     *
     * @param function the function to use to compute the result of the returned CompletionStageItemProcessor
     * @param <V>      the function's return type
     * @return the new CompletionStageItemProcessor
     * @see CompletionStage#thenApply(Function)
     */
    default <V> CompletionStageItemProcessor<T, V> apply(final Function<? super R, ? extends V> function) {
        return apply((a, b) -> function.apply(b));
    }

    /**
     * Returns a new CompletionStageItemProcessor that, when this processor completes normally, completes with this
     * processors's input and result as the arguments to the supplied Bifunction.
     * See the CompletionStage documentation for rules covering exceptional completion.
     *
     * @param function the function to use to compute the result of the returned CompletionStageItemProcessor
     * @param <V>      the function's return type
     * @return the new CompletionStageItemProcessor
     * @see CompletionStage#thenApply(Function)
     */
    default <V> CompletionStageItemProcessor<T, V> apply(final BiFunction<? super T, ? super R, ? extends V> function) {
        return new CompletionStageItemProcessor<>() {
            @Override
            public CompletionStage<Void> prepare() {
                return CompletionStageItemProcessor.this.prepare();
            }

            @Override
            public CompletionStage<V> onNext(T item) {
                return CompletionStageItemProcessor.this.onNext(item).thenApply(r -> function.apply(item, r));
            }
        };
    }

    /**
     * Returns a new CompletionStageItemProcessor that is completed with the result of this
     * CompletionStageItemProcessor And is prepared when this and the passed processor's preparation completes normally.
     * <p>Invoking the returned processor's prepare method, prepares this processor and then the passed processor.
     * <p>When this processor completes normally, the passed processor is invoked with this processor's result.
     * <p>While the passed processor is invoked, it's result is never awaited, and the returned processor completes
     * immediately with this processor's result.
     *
     * @param after Invoked after this processors' processing completes
     * @param <V>   processing result type of the passed CompletionStageItemProcessor
     * @return the new CompletionStageItemProcessor
     * @see #composeAsyncOptionally(Predicate, CompletionStageItemProcessor)
     */
    default <V> CompletionStageItemProcessor<T, R> composeAsync(final CompletionStageItemProcessor<? super R, ?
            extends V> after) {
        return composeAsyncOptionally(v -> true, after);
    }

    /**
     * Returns a new CompletionStageItemProcessor that is completed with the result of this
     * CompletionStageItemProcessor And is prepared when this and the passed processor's preparation completes normally.
     * <p>Invoking the returned processor's prepare method, prepares this processor and then the passed processor.
     * <p>When this processor completes normally, the given predicate is invoked with this processor's result as the
     * argument, deciding whether the passed processor is invoked with this processor's result or not.
     * <p>While the passed processor is invoked, it's result is never awaited, and the returned processor completes
     * immediately with this processor's result.
     *
     * @param predicate Determines if the passed after processor is invoked
     * @param after     Invoked after this processors' processing completes
     * @param <V>       processing result type of the passed CompletionStageItemProcessor
     * @return the new CompletionStageItemProcessor
     * @see #composeAsync(CompletionStageItemProcessor)
     * @see CompletionStage#thenCompose(Function)
     */
    default <V> CompletionStageItemProcessor<T, R> composeAsyncOptionally(final Predicate<? super R> predicate,
                                                                          final CompletionStageItemProcessor<?
                                                                                  super R, ?
                                                                                  extends V> after) {
        Objects.requireNonNull(after);
        return new CompletionStageItemProcessor<>() {
            @Override
            public CompletionStage<Void> prepare() {
                return CompletionStageItemProcessor.this.prepare()
                        .thenCompose(v -> after.prepare());
            }

            @Override
            public CompletionStage<R> onNext(T item) {
                CompletionStage<R> result = CompletionStageItemProcessor.this.onNext(item);
                result.thenCompose(r -> predicate.test(r) ? after.onNext(r) : CompletableFuture.completedStage(null));
                return result;
            }
        };
    }

    /**
     * Returns a new CompletionStageItemProcessor that is completed with the result of the
     * passed CompletionStageItemProcessor And is prepared when this and the passed processor's preparation completes
     * normally.
     * <p>Invoking the returned processor's prepare method, prepares this processor and then the passed processor.
     * <p>When this processor completes normally, the passed processor is invoked with this processor's result.
     *
     * @param after Invoked after this processors' processing completes
     * @param <V>   processing result type of the passed CompletionStageItemProcessor
     * @return the new CompletionStageItemProcessor
     * @see #composeOptionally(Predicate, CompletionStageItemProcessor)
     */
    default <V> CompletionStageItemProcessor<T, V> compose(final CompletionStageItemProcessor<? super R, ? extends V> after) {
        Objects.requireNonNull(after);
        CompletionStageItemProcessor<T, Optional<V>> returnProcessor = composeOptionally(v -> true, after);

        return new CompletionStageItemProcessor<>() {
            @Override
            public CompletionStage<Void> prepare() {
                return returnProcessor.prepare();
            }

            @Override
            public CompletionStage<V> onNext(T item) {
                return returnProcessor.onNext(item).thenApply(o -> o.orElse(null));
            }
        };
    }

    /**
     * Returns a new CompletionStageItemProcessor that is optionally completed with the result of the
     * passed CompletionStageItemProcessor And is prepared when this and the passed processor's preparation completes
     * normally.
     * <p>Invoking the returned processor's prepare method, prepares this processor and then the passed processor.
     * <p>When this processor completes normally, the given predicate is invoked with this processor's result as the
     * argument, deciding whether the passed processor is invoked with this processor's result or not.
     * <p> If the passed processor is invoked, the returned processor completes with it's result, else returns an empty
     * optional.
     *
     * @param predicate Determines if the passed after processor is invoked
     * @param after     Invoked after this processors' processing completes
     * @param <V>       processing result type of the passed CompletionStageItemProcessor
     * @return the new CompletionStageItemProcessor
     * @see #compose(CompletionStageItemProcessor)
     * @see CompletionStage#thenCompose(Function)
     */
    default <V> CompletionStageItemProcessor<T, Optional<V>> composeOptionally(final Predicate<? super R> predicate,
                                                                               final CompletionStageItemProcessor<
                                                                                       ? super R,
                                                                                       ? extends V> after) {
        Objects.requireNonNull(after);
        return new CompletionStageItemProcessor<>() {
            @Override
            public CompletionStage<Void> prepare() {
                return CompletionStageItemProcessor.this.prepare()
                        .thenCompose(v -> after.prepare());
            }

            @Override
            public CompletionStage<Optional<V>> onNext(T item) {
                return CompletionStageItemProcessor.this.onNext(item)
                        .thenCompose(r -> predicate.test(r) ? after.onNext(r)
                                .thenApply(Optional::ofNullable) : CompletableFuture.completedStage(Optional.empty()));
            }
        };
    }

}
