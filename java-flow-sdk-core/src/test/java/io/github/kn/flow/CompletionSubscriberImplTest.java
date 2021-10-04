package io.github.kn.flow;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.kn.flow.util.MockCompletionSubscription;

/**
 *
 */
class CompletionSubscriberImplTest {

    private CompletionSubscriberImpl<String> subscriber;
    private MockCompletionStageItemProcessor mockProcessor;
    private MockCompletionSubscription mockCompletionSubscription;

    @BeforeEach
    void setUp() {
        mockCompletionSubscription = new MockCompletionSubscription();
        mockProcessor = new MockCompletionStageItemProcessor();
        subscriber = new CompletionSubscriberImpl<>(mockProcessor, 1, 1);
        subscriber.onSubscribe(mockCompletionSubscription);
    }

    @Test
    void onSubscribePreparesProcessorAndRequestsMoreItems() {
        Assertions.assertEquals(1, mockProcessor.getTimesPrepareInvoked());
        Assertions.assertEquals("[1]", mockCompletionSubscription.getRequestArgs().toString());
    }

    @Test
    void onNextRequestsMoreItems() {
        subscriber.onNext("testItem");
        Assertions.assertEquals("[1, 1]", mockCompletionSubscription.getRequestArgs().toString());
    }


    @Test
    void onNextRequestsProcessesItems() {
        subscriber.onNext("testItem");
        Assertions.assertEquals("testItem;", mockProcessor.getItems());
    }

    @Test
    void noMoreThanMaxConcurrencyItemsAreRequested() {
        mockProcessor.setOnNextCompletionStage(new CompletableFuture<>());
        subscriber.onNext("testItem");
        subscriber.onNext("testItem1");
        Assertions.assertEquals("testItem;testItem1;", mockProcessor.getItems());
        Assertions.assertEquals("[1, 1]", mockCompletionSubscription.getRequestArgs().toString());
    }

    @Test
    void prepareProcessorExceptionCancelsSubscription() {
        mockCompletionSubscription = new MockCompletionSubscription();
        mockProcessor.setPrepareCompletionStage(CompletableFuture.failedStage(new RuntimeException()));
        new CompletionSubscriberImpl<>(mockProcessor).onSubscribe(mockCompletionSubscription);
        Assertions.assertEquals(1, mockCompletionSubscription.getTimesCancelInvoked());
    }


    @Test
    void itemProcessingExceptionCancelsSubscription() {
        mockProcessor.setOnNextCompletionStage(CompletableFuture.failedStage(new RuntimeException()));
        subscriber.onNext("testItem");
        Assertions.assertEquals(1, mockCompletionSubscription.getTimesCancelInvoked());
    }

    @Test
    void itemProcessingIllegalArgumentExceptionDoesNotCancelSubscription() {
        mockProcessor.setOnNextCompletionStage(CompletableFuture.failedStage(new IllegalArgumentException()));
        subscriber.onNext("testItem");
        Assertions.assertEquals(0, mockCompletionSubscription.getTimesCancelInvoked());
    }

    @Test
    void backPressureChunkSizeTwo() {
        mockCompletionSubscription = new MockCompletionSubscription();
        subscriber = new CompletionSubscriberImpl<>(mockProcessor, 1, 2);
        subscriber.onSubscribe(mockCompletionSubscription);
        subscriber.onNext("testItem");
        Assertions.assertEquals("[2, 2]", mockCompletionSubscription.getRequestArgs().toString());
    }

    @Test
    void onCompleteNoItems() {
        Assertions.assertEquals(0, mockCompletionSubscription.getTimesOnCompleteInvoked());
        subscriber.onComplete();
        Assertions.assertEquals(1, mockCompletionSubscription.getTimesOnCompleteInvoked());
    }

    @Test
    void onCompleteAfterAllRequestsProcessed() {
        subscriber.onNext("testItem");
        subscriber.onComplete();
        Assertions.assertEquals(1, mockCompletionSubscription.getTimesOnCompleteInvoked());
    }

    @Test
    void onCompleteBeforeAllRequestsProcessed() {
        mockProcessor.setOnNextCompletionStage(new CompletableFuture<>());
        subscriber.onNext("testItem");
        subscriber.onNext("testItem1");
        subscriber.onComplete();
        Assertions.assertEquals(0, mockCompletionSubscription.getTimesOnCompleteInvoked());
    }


    @Test
    void onError() {
    }


    private static class MockCompletionStageItemProcessor implements CompletionStageItemProcessor<String, Void> {

        private StringBuilder items = new StringBuilder();
        private int timesPrepareInvoked;
        private CompletionStage<Void> prepareCompletionStage = CompletableFuture.completedStage(null);
        private CompletionStage<Void> onNextCompletionStage = CompletableFuture.completedStage(null);

        public String getItems() {
            return items.toString();
        }

        public int getTimesPrepareInvoked() {
            return timesPrepareInvoked;
        }

        public MockCompletionStageItemProcessor setPrepareCompletionStage(CompletionStage<Void> prepareCompletionStage) {
            this.prepareCompletionStage = prepareCompletionStage;
            return this;
        }

        public MockCompletionStageItemProcessor setOnNextCompletionStage(CompletionStage<Void> onNextCompletionStage) {
            this.onNextCompletionStage = onNextCompletionStage;
            return this;
        }

        @Override
        public CompletionStage<Void> prepare() {
            this.timesPrepareInvoked++;
            return this.prepareCompletionStage;
        }

        @Override
        public CompletionStage<Void> onNext(String item) {
            items.append(item).append(";");
            return onNextCompletionStage;
        }
    }
}