package io.github.kn.flow;

import java.util.concurrent.Flow;

/**
 * A {@link java.util.concurrent.Flow.Subscription} which additionally signals completion of ALL
 * the downstream (subscription) processing. This is useful when subscription item processing is
 * inherently non-blocking/Asynchronous i.e. where {@link java.util.concurrent.Flow.Subscriber#onNext(Object)}
 * might have triggered a future completable processing
 * (and hypothetically returned a {@link java.util.concurrent.CompletableFuture} instance) and one needs to
 * act/await upon completion of all the associated future tasks.
 * <br>
 * The implementations would invoke {@link #onComplete()}, once the ALL the future processing(s) associated with the
 * {@link java.util.concurrent.Flow.Subscription} are complete.
 * Note that {@link #cancel()} may or may not preempt {@link #onComplete()}.
 */
public interface CompletionSubscription extends Flow.Subscription {

    /**
     * Invoked once the subscription item processing for ALL items
     * {@link java.util.concurrent.Flow.Subscriber#onNext(Object)} completes (in Future).
     * Implementors may choose to complete a {@link java.util.concurrent.CompletionStage} on being invoked.
     */
    void onComplete();

}
