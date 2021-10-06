package io.github.kn.flow.util;

import java.util.ArrayList;
import java.util.List;

import io.github.kn.flow.CompletionSubscriber;
import io.github.kn.flow.CompletionSubscription;

public class MockCompletionSubscriber<T> implements CompletionSubscriber<T> {

    private final List<T> itemsList = new ArrayList<>();
    private int timesCompleteInvoked;
    private CompletionSubscription subscription;
    private long requestSize = 1;
    private Throwable onErrorArg;

    public CompletionSubscription getSubscription() {
        return subscription;
    }

    public MockCompletionSubscriber<T> setRequestSize(long requestSize) {
        this.requestSize = requestSize;
        return this;
    }

    public int getTimesCompleteInvoked() {
        return timesCompleteInvoked;
    }

    public List<T> getItemsList() {
        return itemsList;
    }

    @Override
    public void onSubscribe(CompletionSubscription subscription) {
        this.subscription = subscription;
        subscription.request(requestSize);
    }

    @Override
    public void onNext(T item) {
        itemsList.add(item);
        this.subscription.request(requestSize);
    }

    @Override
    public void onError(Throwable throwable) {
        this.onErrorArg = throwable;
    }

    @Override
    public void onComplete() {
        timesCompleteInvoked++;
    }

    public Throwable getOnErrorArg() {
        return onErrorArg;
    }
}
