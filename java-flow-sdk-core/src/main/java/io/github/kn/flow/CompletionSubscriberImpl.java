package io.github.kn.flow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link CompletionSubscriber} employing configured back-pressure and
 * concurrency limits while delegating subscribed items to
 * {@link CompletionStageItemProcessor}. <br>
 * concurrency limit/maxConcurrency - maximum number of items that can have their processing
 * outstanding at any given time. <br>
 * backpressuresize/chunkSize - number of items demanded in one batch from
 * upstream publisher once an item processing is complete. (i.e. input parameter
 * of {@link java.util.concurrent.Flow.Subscription#request(long)}).
 */
public class CompletionSubscriberImpl<T> implements CompletionSubscriber<T> {

	public static final int MAX_PROCESSING_CONCURRENCY = 1000;
	public static final int BACK_PRESSURE_CHUNK_SIZE = 1;
	private static final Logger LOG = LogManager.getLogger("CompletionSubscriberImpl");
	private final CompletionStageItemProcessor<T, Void> itemProcessor;
	private final AtomicInteger outstandingAsyncRequests = new AtomicInteger(0);
	private final AtomicBoolean moreItems = new AtomicBoolean(false);
	private final int backPressureSize;
	private final int maxProcessingConcurrency;
	private volatile CompletionSubscription subscription;

	/**
	 * @param processor
	 */
	CompletionSubscriberImpl(final CompletionStageItemProcessor<T, Void> processor) {
		this(processor, MAX_PROCESSING_CONCURRENCY, BACK_PRESSURE_CHUNK_SIZE);
	}

	CompletionSubscriberImpl(final CompletionStageItemProcessor<T, Void> processor, final int maxConcurrency,
			final int chunkSize) {
		this.itemProcessor = processor;
		this.maxProcessingConcurrency = maxConcurrency;
		this.backPressureSize = chunkSize;
	}

	/**
	 * @param processor
	 * @param maxConcurrency
	 * @param chunkSize
	 * @param <T>
	 * @return
	 */
	public static <T> CompletionSubscriberImpl<T> wrap(final CompletionStageItemProcessor<T, Void> processor,
			final int maxConcurrency, final int chunkSize) {
		return new CompletionSubscriberImpl<>(processor, maxConcurrency, chunkSize);
	}

	@Override
	public void onSubscribe(final CompletionSubscription subs) {
		subscription = subs;
		outstandingAsyncRequests.set(0);
		moreItems.set(true);
		this.itemProcessor.prepare().thenRun(() -> this.subscription.request(backPressureSize))
				.exceptionally(this::handleException);
	}

	@Override
	public void onNext(final T item) {
		LOG.trace("onNext invoked for item {} for: {}", item, itemProcessor);
		boolean requestMoreConcurrently = outstandingAsyncRequests.incrementAndGet() <= maxProcessingConcurrency;
		this.itemProcessor.onNext(item).exceptionally(this::handleException).whenComplete((r, t) -> {
			Optional.ofNullable(t).ifPresent(e -> LOG.error("Exception processing item" + item, e));
			requestMoreOrCompleteSubscription(outstandingAsyncRequests.decrementAndGet(), moreItems.get(),
					!requestMoreConcurrently);
		});
		requestMoreOrCompleteSubscription(outstandingAsyncRequests.get(), moreItems.get(), requestMoreConcurrently);
	}

	private void requestMoreOrCompleteSubscription(final int numOutstandingRequests, final boolean hasMoreItems,
			final boolean requestMore) {
		if (numOutstandingRequests <= 0 && !hasMoreItems) {
			LOG.trace("No more items to publish for:{}", itemProcessor);
			this.subscription.onComplete();
		} else if (requestMore) {
			this.subscription.request(backPressureSize);
		}
	}

	private Void handleException(final Throwable throwable) {
		Optional.ofNullable(throwable).ifPresent(t -> {
			if (t instanceof IllegalArgumentException || t.getCause() instanceof IllegalArgumentException) {
				LOG.error("Message processing failed for:" + this.itemProcessor, t);
			} else {
				LOG.error(itemProcessor + " exception... cancelling subscription", throwable);
				this.subscription.cancel();
			}
		});
		return null;
	}

	@Override
	public void onError(final Throwable throwable) {
		requestMoreOrCompleteSubscription(outstandingAsyncRequests.get(), !moreItems.compareAndSet(true, false), false);
		LOG.error("onError invoked for:" + this.itemProcessor, throwable);
	}

	@Override
	public void onComplete() {
		requestMoreOrCompleteSubscription(outstandingAsyncRequests.get(), !moreItems.compareAndSet(true, false), false);
		LOG.info("onComplete invoked for:{}", this.itemProcessor);
	}
}