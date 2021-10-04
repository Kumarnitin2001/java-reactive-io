package io.github.kn.flow;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link CompletionSubscriber} of bytes ({@link ByteBuffer}) invoking a wrapped downstream textline
 * (String) {@link CompletionSubscriber}.
 * <br>
 * Conversion of bytes to text line is as per {@link HttpResponse.BodySubscribers#fromLineSubscriber(Flow.Subscriber)}
 */
public class ByteBufferToTextLineCompletionSubscriberAdapter implements CompletionSubscriber<List<ByteBuffer>> {

    private static final Logger LOG = LogManager.getLogger("ByteBufferToTextLineCompletionSubscriberAdapter");

    private final CompletionSubscriber<String> subscriberDelegate;
    private final String lineSeparator;
    private final Charset charset;
    private volatile HttpResponse.BodySubscriber<Void> bytesArraySubscriberDelegate;

    ByteBufferToTextLineCompletionSubscriberAdapter(final CompletionSubscriber<String> delegate, final Charset cSet,
                                                    final String separator) {
        this.subscriberDelegate = delegate;
        this.charset = cSet;
        this.lineSeparator = separator;
    }

    /**
     * @param delegate
     * @param cSet
     * @param separator
     * @return
     */
    public static ByteBufferToTextLineCompletionSubscriberAdapter adapt(final CompletionSubscriber<String> delegate,
                                                                        final Charset cSet,
                                                                        final String separator) {
        return new ByteBufferToTextLineCompletionSubscriberAdapter(delegate, cSet, separator);
    }

    @Override
    public void onSubscribe(final CompletionSubscription subs) {
        LOG.trace("init invoked");
        this.bytesArraySubscriberDelegate = HttpResponse.BodySubscribers
                .fromLineSubscriber(new Flow.Subscriber<>() {
                    private volatile AtomicInteger publishCount = new AtomicInteger(0);

                    @Override
                    public void onSubscribe(final Flow.Subscription subscription) {
                        subscriberDelegate.onSubscribe(new AbstractCompletionSubscription(subscription) {
                            @Override
                            public void onComplete() {
                                subs.onComplete();
                                LOG.debug("Subscription completed by subscriber {} after publishing {} messages",
                                        subscriberDelegate, publishCount);
                            }

                            @Override
                            public void cancel() {
                                super.cancel();
                                LOG.warn("Subscription cancelled by subscriber {} after publishing {} messages",
                                        subscriberDelegate, publishCount);
                            }
                        });
                    }

                    @Override
                    public void onNext(String item) {
                        this.publishCount.incrementAndGet();
                        subscriberDelegate.onNext(item);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        subscriberDelegate.onError(throwable);
                        LOG.error("onError invoked after publishing " + publishCount + " messages for:" + subscriberDelegate,
                                throwable);
                    }

                    @Override
                    public void onComplete() {
                        subscriberDelegate.onComplete();
                        LOG.info("onComplete invoked after publishing {} messages for:{}", publishCount,
                                subscriberDelegate);
                    }

                }, s -> null, charset, lineSeparator);
        this.bytesArraySubscriberDelegate.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
                LOG.debug("Requesting {} more items of bytes array", n);
                subs.request(n);
            }

            @Override
            public void cancel() {
                subs.cancel();
                LOG.debug("subscription cancelled by subscriber");
            }
        });
    }

    @Override
    public void onNext(final List<ByteBuffer> byteBuffers) {
        LOG.trace("onNext {}", byteBuffers);
        this.bytesArraySubscriberDelegate.onNext(byteBuffers);
    }

    @Override
    public void onComplete() {
        LOG.debug("onComplete");
        this.bytesArraySubscriberDelegate.onComplete();
    }

    @Override
    public void onError(Throwable throwable) {
        LOG.error("onError bytes Array subscription", throwable);
        this.bytesArraySubscriberDelegate.onError(throwable);
    }
}