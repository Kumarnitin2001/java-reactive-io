package io.github.kn.flow.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import rawhttp.core.*;
import rawhttp.core.body.BodyReader;
import rawhttp.core.body.EagerBodyReader;

import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;

/**
 * A codec of HTTPRequest and HttpResponse Objects to/from their string representation
 * as per <a href="https://tools.ietf.org/html/rfc2616#section-5"> rfc2616#section-5</a>
 * ,<a href="https://tools.ietf.org/html/rfc2616#section-6"> rfc2616#section-6</a>
 */
public class HttpRequestResponseStringCodec {

    public static final Set<String> DISALLOWED_HEADERS_SET;
    static final String CONTENT_LENGTH = "content-length";
    private static final Logger LOG = LogManager.getLogger("HttpRequestResponseStringCodec");
    private static final RawHttp RAW_HTTP_DELEGATE = new RawHttp();
    private static final String SPACE = " ";

    static {
        // A case insensitive TreeSet of strings.
        TreeSet<String> treeSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        treeSet.addAll(Set.of("connection", CONTENT_LENGTH,
                "date", "expect", "wrap", "host", "upgrade", "via", "warning"));
        DISALLOWED_HEADERS_SET = Collections.unmodifiableSet(treeSet);
    }

    private final HttpMessage message;

    private HttpRequestResponseStringCodec(final HttpMessage message) {
        this.message = message;
    }

    /**
     * Encodes string as per <a href="https://tools.ietf.org/html/rfc2616#section-5"> rfc2616#section-5</a>
     *
     * @param httpRequest object to encode as string
     * @return the encoded form
     */
    public static HttpRequestResponseStringCodec fromHttpRequest(final HttpRequest httpRequest) {
        EagerBodyReader eagerReader = from(httpRequest.bodyPublisher());
        Map<String, List<String>> headersMap = new HashMap<>(httpRequest.headers().map());
        headersMap.computeIfAbsent(CONTENT_LENGTH, k -> Arrays
                .asList(String.valueOf(eagerReader.getLengthIfKnown().orElse(0l))));

        RawHttpRequest rawRequest = new RawHttpRequest(new RequestLine(httpRequest.method(), httpRequest
                .uri(), HttpVersion.valueOf(httpRequest.version().orElse(HttpClient.Version.HTTP_1_1).name())) {
            @Override
            public String toString() {
                return new StringBuilder().append(this.getMethod()).append(SPACE).append(this.getUri()).append(SPACE)
                        .append(this.getHttpVersion()).toString();
            }
        }, from(headersMap), eagerReader, null);
        return new HttpRequestResponseStringCodec(rawRequest);
    }

    private static RawHttpHeaders from(final Map<String, List<String>> headers) {
        RawHttpHeaders.Builder rawHttpHeadersBuilder = RawHttpHeaders.newBuilder();
        headers.forEach((a, l) -> l.stream().forEach(v -> rawHttpHeadersBuilder.with(a, v)));
        return rawHttpHeadersBuilder.build();
    }

    /**
     * Decodes string as per <a href="https://tools.ietf.org/html/rfc2616#section-5"> rfc2616#section-5</a>
     *
     * @param reqStr String form to decode into HTTPRequest
     * @return the decoded object
     */
    public static HttpRequest toHttpRequest(final String reqStr) {
        try {
            RawHttpRequest rawHttpRequest = RAW_HTTP_DELEGATE.parseRequest(reqStr).eagerly();

            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(rawHttpRequest.getUri())
                    .version(HttpClient.Version.valueOf(rawHttpRequest.getStartLine().getHttpVersion().name()))
                    .method(rawHttpRequest.getMethod(), HttpRequest.BodyPublishers
                            .ofInputStream(() -> rawHttpRequest.getBody().map(BodyReader::asRawStream)
                                    .orElse(InputStream.nullInputStream())));
            rawHttpRequest.getHeaders().asMap().entrySet()
                    .stream().filter(e -> !DISALLOWED_HEADERS_SET.contains(e.getKey().toLowerCase()))
                    .forEach((e) -> e.getValue().stream().forEach(v -> requestBuilder.header(e.getKey(), v)));
            return requestBuilder.build();
        } catch (Exception e) {
            throw new IllegalArgumentException("Exception parsing " + reqStr, e);
        }
    }

    /**
     * Encodes string as per <a href="https://tools.ietf.org/html/rfc2616#section-6"> rfc2616#section-6</a>
     *
     * @param response HttpResponse object to encode as string
     * @return the encoded form
     */
    public static HttpRequestResponseStringCodec fromHttpResponse(final HttpResponse<byte[]> response) {
        StatusLine statusLine = new StatusLine(HttpVersion.valueOf(response.version().name()), response
                .statusCode(), "");
        return new HttpRequestResponseStringCodec(
                new RawHttpResponse<String>(null, null, statusLine, from(response
                        .headers().map()), new EagerBodyReader(Optional.ofNullable(response.body())
                        .orElse(new byte[0]))));
    }

    private static EagerBodyReader from(final Optional<HttpRequest.BodyPublisher> bodyPublisher) {
        return bodyPublisher.map(HttpRequestResponseStringCodec::from)
                .orElse(new EagerBodyReader(new byte[0]));
    }

    private static EagerBodyReader from(final HttpRequest.BodyPublisher publisher) {
        try {
            HttpResponse.BodySubscriber<byte[]> requestBytes = HttpResponse.BodySubscribers
                    .ofByteArray();
            publisher
                    .subscribe(ByteBufferListToByteBufferSubscriberAdapter
                            .adapt(requestBytes));
            return new EagerBodyReader(requestBytes.getBody().toCompletableFuture()
                    .get());
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Exception reading body", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return String.valueOf(this.message);
    }

    private static class ByteBufferListToByteBufferSubscriberAdapter implements Flow.Subscriber<ByteBuffer> {
        private final Flow.Subscriber<List<ByteBuffer>> delegate;

        private ByteBufferListToByteBufferSubscriberAdapter(final Flow.Subscriber<List<ByteBuffer>> delegate) {
            this.delegate = delegate;
        }

        public static ByteBufferListToByteBufferSubscriberAdapter adapt(final Flow.Subscriber<List<ByteBuffer>> delegate) {
            return new ByteBufferListToByteBufferSubscriberAdapter(delegate);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            delegate.onSubscribe(subscription);
        }

        @Override
        public void onNext(ByteBuffer item) {
            delegate.onNext(Arrays.asList(item));
        }

        @Override
        public void onError(Throwable throwable) {
            delegate.onError(throwable);
        }

        @Override
        public void onComplete() {
            delegate.onComplete();
        }
    }

}
