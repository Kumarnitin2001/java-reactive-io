package io.github.kn.flow.util;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;

public class MockHttpClient extends HttpClient {

    private final StringBuilder requestBody = new StringBuilder();
    private HttpResponse.BodyHandler<?> responseBodyHandlerArg;
    private HttpRequest requestArg;
    private int responseStatusCode;
    private Map<String, List<String>> headers = new HashMap<>();
    private Object body;
    private Optional<Exception> completionException = Optional.empty();
    private Optional<RuntimeException> exception = Optional.empty();
    private HttpResponse.PushPromiseHandler<?> pushPromiseHandlerArg;

    public MockHttpClient setRuntimeException(RuntimeException exception) {
        this.exception = Optional.of(exception);
        return this;
    }

    public StringBuilder getRequestBody() {
        return requestBody;
    }

    public HttpResponse.PushPromiseHandler<?> getPushPromiseHandlerArg() {
        return pushPromiseHandlerArg;
    }

    public MockHttpClient setResponseStatusCode(int responseStatusCode) {
        this.responseStatusCode = responseStatusCode;
        return this;
    }

    public MockHttpClient setHeaders(Map<String, List<String>> headers) {
        this.headers = headers;
        return this;
    }

    public MockHttpClient setBody(Object body) {
        this.body = body;
        return this;
    }

    public MockHttpClient setCompletionException(Exception completionException) {
        this.completionException = Optional.of(completionException);
        return this;
    }

    public HttpResponse.BodyHandler<?> getResponseBodyHandlerArg() {
        return responseBodyHandlerArg;
    }

    public HttpRequest getRequestArg() {
        return requestArg;
    }

    @Override
    public Optional<CookieHandler> cookieHandler() {
        return Optional.empty();
    }

    @Override
    public Optional<Duration> connectTimeout() {
        return Optional.empty();
    }

    @Override
    public Redirect followRedirects() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ProxySelector> proxy() {
        return Optional.empty();
    }

    @Override
    public SSLContext sslContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SSLParameters sslParameters() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Authenticator> authenticator() {
        return Optional.empty();
    }

    @Override
    public Version version() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Executor> executor() {
        return Optional.empty();
    }

    @Override
    public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler) throws IOException, InterruptedException {
        throw new UnsupportedEncodingException();
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
                                                            HttpResponse.BodyHandler<T> responseBodyHandler) {
        return sendAsync(request, responseBodyHandler, null);
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
                                                            HttpResponse.BodyHandler<T> responseBodyHandler,
                                                            HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
        this.exception.ifPresent(e -> {
            throw e;
        });
        this.requestArg = request;
        requestArg.bodyPublisher().ifPresent(pub -> pub.subscribe(new Flow.Subscriber<>() {
            private volatile Flow.Subscription flowSubscription;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                flowSubscription = subscription;
                subscription.request(1);
            }

            @Override
            public void onNext(ByteBuffer item) {
                requestBody.append(StandardCharsets.UTF_8.decode(item));
                flowSubscription.request(1);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {

            }
        }));
        this.responseBodyHandlerArg = responseBodyHandler;
        this.pushPromiseHandlerArg = pushPromiseHandler;
        return completionException.map(e -> CompletableFuture.<HttpResponse<T>>failedFuture(e)).orElse(
                CompletableFuture.completedFuture(new HttpResponse<>() {
                    @Override
                    public int statusCode() {
                        return responseStatusCode;
                    }

                    @Override
                    public HttpRequest request() {
                        return request;
                    }

                    @Override
                    public Optional<HttpResponse<T>> previousResponse() {
                        return Optional.empty();
                    }

                    @Override
                    public HttpHeaders headers() {
                        return HttpHeaders.of(headers, (a, b) -> true);
                    }

                    @Override
                    public T body() {
                        return (T) body;
                    }

                    @Override
                    public Optional<SSLSession> sslSession() {
                        return Optional.empty();
                    }

                    @Override
                    public URI uri() {
                        return request.uri();
                    }

                    @Override
                    public Version version() {
                        return Version.HTTP_1_1;
                    }
                }));
    }
}
