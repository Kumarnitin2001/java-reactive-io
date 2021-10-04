package io.github.kn.flow;

import java.util.concurrent.Flow;

/**
 * Simple adapter adapting a  {@link java.util.concurrent.Flow.Subscription} to {@link CompletionSubscription}
 */
public abstract class AbstractCompletionSubscription implements CompletionSubscription {
    private final Flow.Subscription delegate;

    /**
     * @param dlg {@link java.util.concurrent.Flow.Subscription} to be adapted.
     */
    protected AbstractCompletionSubscription(final Flow.Subscription dlg) {
        this.delegate = dlg;
    }

    @Override
    public void cancel() {
        delegate.cancel();
    }

    @Override
    public void request(long n) {
        delegate.request(n);
    }
}