package io.github.kn.flow;

import java.util.concurrent.Flow;

/**
 * A {@link java.util.concurrent.Flow.Subscriber} supporting {@link CompletionSubscription}
 */
public interface CompletionSubscriber<T> extends Flow.Subscriber<T> {

    /**
     * @param subscription {@link CompletionSubscription}
     */
    void onSubscribe(CompletionSubscription subscription);

    /**
     * @param subscription {@link java.util.concurrent.Flow.Subscription}
     */
    @Override
    default void onSubscribe(Flow.Subscription subscription) {
        onSubscribe(new AbstractCompletionSubscription(subscription) {
            @Override
            public void onComplete() {
                throw new UnsupportedOperationException();
            }
        });
    }
}
