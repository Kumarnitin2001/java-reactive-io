package io.github.kn.flow.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;

/**
 *
 */
public class MockSubscriber<T> implements Flow.Subscriber<T> {

    private List<T> publishedItems = new ArrayList<>();
    private Flow.Subscription subscription;
    private int timesCompleteInvoked;
    private int timesErrorInvoked;

    public List<T> getPublishedItems() {
        return publishedItems;
    }

    public int getTimesCompleteInvoked() {
        return timesCompleteInvoked;
    }

    public int getTimesErrorInvoked() {
        return timesErrorInvoked;
    }

    @Override
    public void onSubscribe(Flow.Subscription subs) {
        this.subscription = subs;
        subscription.request(1);
    }

    @Override
    public void onNext(T item) {
        publishedItems.add(item);
        subscription.request(1);
    }

    @Override
    public void onError(Throwable throwable) {
        this.timesErrorInvoked++;
    }

    @Override
    public void onComplete() {
        this.timesCompleteInvoked++;
    }
}
