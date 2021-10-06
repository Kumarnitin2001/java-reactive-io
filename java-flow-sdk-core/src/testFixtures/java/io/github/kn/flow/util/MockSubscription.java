package io.github.kn.flow.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;

/**
 *
 */
public class MockSubscription implements Flow.Subscription {
    private List<Long> requestArgs = new ArrayList<>();
    private int timesCancelInvoked;

    public List<Long> getRequestArgs() {
        return requestArgs;
    }

    public int timesCancelInvoked() {
        return timesCancelInvoked;
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
