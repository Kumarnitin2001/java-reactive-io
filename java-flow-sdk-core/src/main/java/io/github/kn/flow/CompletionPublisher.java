package io.github.kn.flow;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

/**
 * A publisher exclusively supporting {@link CompletionSubscription}. Typically used for implementing publishers
 * which publish finite number items to subscribers {@link CompletionSubscriber}.
 * The subscribe method returns {@link CompletableFuture} which is completed once the processing flow/pipeline completes
 * i.e. all the {@link CompletionSubscriber}s complete after receiving all the published items.
 *
 * @see Flow.Publisher
 */
public interface CompletionPublisher<T> {

    /**
     * @param subscriber {@link CompletionSubscriber}
     * @see java.util.concurrent.Flow.Publisher#subscribe(Flow.Subscriber)
     */
    CompletableFuture<Void> subscribe(CompletionSubscriber<? super T> subscriber);
}
