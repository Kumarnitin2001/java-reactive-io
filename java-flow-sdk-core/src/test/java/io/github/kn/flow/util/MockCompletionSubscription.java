package io.github.kn.flow.util;

import java.util.ArrayList;
import java.util.List;

import io.github.kn.flow.CompletionSubscription;

/**
 *
 */
public class MockCompletionSubscription implements CompletionSubscription {
    private int timesOnCompleteInvoked;
    private List<Long> requestArgs = new ArrayList<>();
    private int timesCancelInvoked;

    public int getTimesOnCompleteInvoked() {
        return timesOnCompleteInvoked;
    }

    public List<Long> getRequestArgs() {
        return requestArgs;
    }

    public int getTimesCancelInvoked() {
        return timesCancelInvoked;
    }

    @Override
    public void onComplete() {
        this.timesOnCompleteInvoked++;

    }

    @Override
    public void request(long n) {
        this.requestArgs.add(n);

    }

    @Override
    public void cancel() {
        this.timesCancelInvoked++;
    }
}
