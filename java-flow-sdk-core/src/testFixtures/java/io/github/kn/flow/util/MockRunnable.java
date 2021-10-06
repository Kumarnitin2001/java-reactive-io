package io.github.kn.flow.util;

/**
 *
 */
public class MockRunnable implements Runnable {
    private int timesRunInvoked;

    public int getTimesRunInvoked() {
        return timesRunInvoked;
    }

    @Override
    public void run() {
        timesRunInvoked++;

    }
}