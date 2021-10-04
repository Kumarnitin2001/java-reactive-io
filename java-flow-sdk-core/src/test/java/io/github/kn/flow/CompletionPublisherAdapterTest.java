package io.github.kn.flow;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.kn.flow.util.MockCompletionSubscriber;
import io.github.kn.flow.util.MockCompletionSubscription;

/**
 *
 */
class CompletionPublisherAdapterTest {

    private CompletionPublisherAdapter<String> publisher;

    private MockCompletionSubscriber<String> mockSubscriber;

    private CompletableFuture<Void> completableFuture;

    private MockCompletionSubscription mockSubscription = new MockCompletionSubscription();
    private MockCompletionPublisher<String> mockDelegate;

    @BeforeEach
    void setUp() {
        mockSubscriber = new MockCompletionSubscriber<>();
        mockDelegate = new MockCompletionPublisher<>();
        publisher = CompletionPublisherAdapter.adapt(mockDelegate);
        completableFuture = publisher.subscribe(mockSubscriber);
    }

    @Test
    void subscribeSubscribesDelegate() {
        Assertions.assertNotNull(mockDelegate.getSubscribeArg());
    }

    @Test
    void onNext() {
        Assertions.assertEquals("[]", mockSubscriber.getItemsList().toString());
        mockDelegate.getSubscribeArg().onNext("testItem");
        Assertions.assertEquals("[testItem]", mockSubscriber.getItemsList().toString());
    }

    @Test
    void onError() {
        Assertions.assertFalse(completableFuture.isDone());
        mockDelegate.getSubscribeArg().onError(new IllegalArgumentException());
        Assertions.assertEquals(IllegalArgumentException.class, mockSubscriber.getOnErrorArg().getClass());
        Assertions.assertTrue(completableFuture.isCompletedExceptionally());
    }

    @Test
    void onCompletion() {
        Assertions.assertEquals(0, mockSubscriber.getTimesCompleteInvoked());
        mockDelegate.getSubscribeArg().onComplete();
        Assertions.assertEquals(1, mockSubscriber.getTimesCompleteInvoked());
    }

    @Test
    void onSubscriptionCancel() {
        Assertions.assertFalse(completableFuture.isDone());
        Assertions.assertEquals(0, mockSubscription.getTimesCancelInvoked());
        mockSubscriber.getSubscription().cancel();
        Assertions.assertEquals(1, mockSubscription
                .getTimesCancelInvoked());
        Assertions.assertTrue(completableFuture.isCompletedExceptionally());
    }

    @Test
    void onSubscriptionCompletion() {
        Assertions.assertFalse(completableFuture.isDone());
        mockSubscriber.getSubscription().onComplete();
        Assertions.assertTrue(completableFuture.isDone());
    }


    private class MockCompletionPublisher<T> implements Flow.Publisher<T> {

        private Flow.Subscriber<? super T> subscribeArg;

        public Flow.Subscriber<? super T> getSubscribeArg() {
            return subscribeArg;
        }

        @Override
        public void subscribe(Flow.Subscriber<? super T> subscriber) {
            subscriber.onSubscribe(mockSubscription);
            this.subscribeArg = subscriber;
        }

    }
}