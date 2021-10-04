package io.github.kn.flow;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

/**
 * Adapts {@link java.util.concurrent.Flow.Publisher} to {@link CompletionPublisher}.
 */
public class CompletionPublisherAdapter<T> implements CompletionPublisher<T> {

    private final Flow.Publisher<T> delegatePublisher;

    private CompletionPublisherAdapter(final Flow.Publisher<T> delegate) {
        delegatePublisher = delegate;
    }

    public static <T> CompletionPublisherAdapter<T> adapt(final Flow.Publisher<T> publisher) {
        return new CompletionPublisherAdapter<>(publisher);
    }

    @Override
    public CompletableFuture<Void> subscribe(final CompletionSubscriber<? super T> subscriber) {
        final CompletableFuture<Void> completionFuture = new CompletableFuture<>();
        delegatePublisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(final Flow.Subscription subscription) {
                subscriber.onSubscribe(new AbstractCompletionSubscription(subscription) {
                    @Override
                    public void onComplete() {
                        completionFuture.complete(null);
                    }

                    @Override
                    public void cancel() {
                        super.cancel();
                        completionFuture
                                .completeExceptionally(new RuntimeException("Subscription closed abruptly, " +
                                        "most likely " +
                                        "due to processing error"));
                    }
                });
            }

            @Override
            public void onNext(T item) {
                subscriber.onNext(item);
            }

            @Override
            public void onError(Throwable throwable) {
                subscriber.onError(throwable);
                completionFuture.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                subscriber.onComplete();
            }
        });
        return completionFuture;
    }
}
