package io.github.kn.flow.aws;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;
import software.amazon.awssdk.http.async.SdkHttpContentPublisher;

/**
 * A custom {@link SdkAsyncHttpClient} using java native
 * {@link java.net.http.HttpClient} as the wrapped/underlying HTTP client
 * library. Allows concrete implementors to translate AWS-SDK native
 * {@link software.amazon.awssdk.http} library calls to java native
 * {@link java.net.http} library calls.
 */
public abstract class AbstractJavaNativeSdkHttpClient implements SdkAsyncHttpClient {
	private static final Logger LOG = LogManager.getLogger("AbstractJavaNativeSdkHttpClient");
	protected static final Subscription EMPTY_SUBSCRIPTION = new Subscription() {
		@Override
		public void request(long l) {

		}

		@Override
		public void cancel() {

		}
	};
	public static final Set<String> IGNORE_HEADERS;

	static {
		// A case insensitive TreeSet of strings.
		TreeSet<String> treeSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
		treeSet.addAll(Set.of("connection", "content-length", "date", "expect", "fromHttpRequest", "host", "upgrade",
				"via", "warning"));
		IGNORE_HEADERS = Collections.unmodifiableSet(treeSet);
	}

	protected HttpRequest adapt(final AsyncExecuteRequest sdkRequest) {
		SdkHttpRequest sdkHttpRequest = sdkRequest.request();

		HttpRequest.Builder requestBuilder = HttpRequest.newBuilder().uri(sdkHttpRequest.getUri()).method(
				sdkHttpRequest.method().toString(),
				AbstractJavaNativeSdkHttpClient.BodyPublisherImpl.adapt(sdkRequest.requestContentPublisher()));

		sdkHttpRequest.headers().entrySet().stream()
				.filter(e -> !IGNORE_HEADERS.contains(e.getKey().toLowerCase()))
				.flatMap(e -> e.getValue().stream().map(v -> Map.entry(e.getKey(), v)))
				.forEach(e -> requestBuilder.header(e.getKey(), e.getValue()));
		return requestBuilder.build();
	}

	private <T> CompletableFuture<HttpResponse<T>> handleHttpError(
			final CompletableFuture<HttpResponse<T>> httpResponse,
			final SdkAsyncHttpResponseHandler sdkAsyncHttpResponseHandler) {
		return httpResponse.whenComplete((r, e) -> {
			if (e != null) {
				sdkAsyncHttpResponseHandler.onError(e);
				LOG.error("HTTP request exception", e);
			}
		});
	}

	private <T> SdkHttpFullResponse.Builder createSdkResponseBuilder(final HttpResponse<T> httpResponse) {
		return SdkHttpFullResponse.builder().statusCode(httpResponse.statusCode())
				.headers(httpResponse.headers().map());

	}

	protected CompletableFuture<Void> prepareByteArrayBodyHandler(
			final CompletableFuture<HttpResponse<byte[]>> httpResponse,
			final SdkAsyncHttpResponseHandler sdkAsyncHttpResponseHandler) {
		return handleHttpError(httpResponse, sdkAsyncHttpResponseHandler).thenAccept(r -> {
			sdkAsyncHttpResponseHandler.onHeaders(createSdkResponseBuilder(r).build());
			sdkAsyncHttpResponseHandler.onStream(s -> {
				s.onSubscribe(EMPTY_SUBSCRIPTION);
				s.onNext(ByteBuffer.wrap(r.body()));
				s.onComplete();
			});
		});
	}

	protected CompletableFuture<HttpResponse<Flow.Publisher<List<ByteBuffer>>>> prepareBytePublisherBodyHandler(
			final CompletableFuture<HttpResponse<Flow.Publisher<List<ByteBuffer>>>> httpResponse,
			final SdkAsyncHttpResponseHandler sdkAsyncHttpResponseHandler) {
		return handleHttpError(httpResponse, sdkAsyncHttpResponseHandler).thenApply(r -> {
			sdkAsyncHttpResponseHandler.onHeaders(createSdkResponseBuilder(r).build());
			sdkAsyncHttpResponseHandler.onStream(s -> {
				s.onSubscribe(EMPTY_SUBSCRIPTION);
				s.onComplete();
			});
			return r;
		});
	}

	@Override
	public void close() {

	}

	private static final class BodyPublisherImpl implements HttpRequest.BodyPublisher {

		private final SdkHttpContentPublisher publisherDelegate;

		private BodyPublisherImpl(SdkHttpContentPublisher publisherDelegate) {
			this.publisherDelegate = publisherDelegate;
		}

		static BodyPublisherImpl adapt(SdkHttpContentPublisher publisherDelegate) {
			return new BodyPublisherImpl(publisherDelegate);
		}

		@Override
		public long contentLength() {
			return publisherDelegate.contentLength().orElse(0l);
		}

		@Override
		public void subscribe(final Flow.Subscriber<? super ByteBuffer> subscriber) {
			publisherDelegate.subscribe(new Subscriber<>() {
				@Override
				public void onSubscribe(final Subscription subscription) {
					subscriber.onSubscribe(new Flow.Subscription() {

						@Override
						public void request(long l) {
							subscription.request(l);
						}

						@Override
						public void cancel() {
							subscription.cancel();
						}
					});
				}

				@Override
				public void onNext(ByteBuffer byteBuffer) {
					subscriber.onNext(byteBuffer);
				}

				@Override
				public void onError(Throwable throwable) {
					subscriber.onError(throwable);
				}

				@Override
				public void onComplete() {
					subscriber.onComplete();
				}
			});
		}
	}

}
