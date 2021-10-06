package io.github.kn.flow.util;

import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class MockSubmissionPublisher<T> extends SubmissionPublisher<T> {
	private Flow.Subscriber subscribeArg;
	private volatile int maxRunCount = 1;
	private AtomicInteger runCount = new AtomicInteger(0);

	public MockSubmissionPublisher(Executor exec, int capacity) {
		super(exec, capacity);
	}

	public MockSubmissionPublisher<T> setMaxRunCount(int maxRunCount) {
		this.maxRunCount = maxRunCount;
		return this;
	}

	public  Flow.Subscriber getSubscribeArg() {
		return subscribeArg;
	}

	@Override
	public void subscribe(Flow.Subscriber subscriber) {
		this.subscribeArg = subscriber;
		super.subscribe(subscriber);
	}

	@Override
	public int submit(T item) {
		super.submit(item);
		if (runCount.incrementAndGet() >= maxRunCount)
			Thread.currentThread().interrupt();
		return 1;
	}
}