package io.github.kn.flow.aws;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.github.kn.flow.util.MockHttpClient;
import io.github.kn.flow.util.MockSubscriber;
import software.amazon.awssdk.core.internal.http.async.SimpleHttpContentPublisher;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;

class JavaNativeSdkHttpClientTest {

    private JavaNativeSdkHttpClient javaNativeSdkHttpClient;

    private MockHttpClient mockHttpClient;

    private MockSdkHttpRequest mockRequest;

    private MockSdkAsyncHttpResponseHandler mockResponseHandler;
    private AsyncExecuteRequest.Builder mockAsyncExecuteRequestBuilder;

    @BeforeEach
    void setUp() {
        mockHttpClient = new MockHttpClient();
        mockRequest = new MockSdkHttpRequest();
        mockResponseHandler = new MockSdkAsyncHttpResponseHandler();
        mockAsyncExecuteRequestBuilder = AsyncExecuteRequest.builder().request(mockRequest)
                .requestContentPublisher(new SimpleHttpContentPublisher(mockRequest));
        javaNativeSdkHttpClient = new JavaNativeSdkHttpClient(mockHttpClient);
    }

    @Test
    void RequestURI() {
        javaNativeSdkHttpClient.execute(mockAsyncExecuteRequestBuilder.build());
        Assertions.assertEquals(URI.create("http://testHost:8080"), mockHttpClient.getRequestArg().uri());
    }

    @Test
    void RequestMethod() {
        javaNativeSdkHttpClient.execute(mockAsyncExecuteRequestBuilder.build());
        Assertions.assertEquals("GET", mockHttpClient.getRequestArg().method());
    }

    @Test
    void RequestWithContentBody() {
        javaNativeSdkHttpClient
                .execute(mockAsyncExecuteRequestBuilder
                        .requestContentPublisher(new SimpleHttpContentPublisher(mockRequest)).build());
        Assertions.assertEquals(11, mockHttpClient.getRequestArg().bodyPublisher().get().contentLength());
        MockSubscriber<ByteBuffer> mockSubscriber = new MockSubscriber<>();
        mockHttpClient.getRequestArg().bodyPublisher().get().subscribe(mockSubscriber);
        Assertions.assertEquals(1, mockSubscriber.getTimesCompleteInvoked());
        Assertions.assertEquals("testContent", mockSubscriber.getPublishedItems().stream().map(b -> b.array())
                .map(String::new)
                .reduce((a, b) -> a + b).get());
    }

    @Test
    void ResponseStatusCode() {
        mockHttpClient.setResponseStatusCode(200);
        javaNativeSdkHttpClient
                .execute(mockAsyncExecuteRequestBuilder.responseHandler(mockResponseHandler).build());
        Assertions.assertTrue(mockResponseHandler.getSdkHttpResponseArg().isSuccessful());
        Assertions.assertEquals(200, mockResponseHandler.getSdkHttpResponseArg().statusCode());
    }

    @Test
    void ResponseBody() {
        mockHttpClient.setResponseStatusCode(200).setBody("bodyContent".getBytes());
        javaNativeSdkHttpClient
                .execute(mockAsyncExecuteRequestBuilder.responseHandler(mockResponseHandler).build());
        Assertions.assertEquals("bodyContent", mockResponseHandler.getBodyAsString());
    }

    @Test
    void Exception() {
        mockHttpClient.setCompletionException(new IllegalArgumentException());
        javaNativeSdkHttpClient
                .execute(mockAsyncExecuteRequestBuilder.responseHandler(mockResponseHandler)
                        .build());
        Assertions.assertEquals(IllegalArgumentException.class, mockResponseHandler.getThrowableArg().getClass());
    }

    @Test
    void HttpRequestArgs() {
        javaNativeSdkHttpClient.execute(mockAsyncExecuteRequestBuilder
                .requestContentPublisher(new SimpleHttpContentPublisher(mockRequest)).build());
        Assertions.assertEquals("{testHeaderKey=[testHeaderValue]}", mockHttpClient.getRequestArg().headers().map()
                .toString());
        Assertions.assertEquals("testContent", mockHttpClient.getRequestBody().toString());
    }

    private static class MockSdkAsyncHttpResponseHandler implements SdkAsyncHttpResponseHandler {

        private final StringBuilder stringBuilder = new StringBuilder();
        private SdkHttpResponse sdkHttpResponseArg;
        private Throwable throwableArg;

        public Throwable getThrowableArg() {
            return throwableArg;
        }

        public String getBodyAsString() {
            return stringBuilder.toString();
        }

        public SdkHttpResponse getSdkHttpResponseArg() {
            return sdkHttpResponseArg;
        }

        @Override
        public void onHeaders(SdkHttpResponse sdkHttpResponse) {
            this.sdkHttpResponseArg = sdkHttpResponse;
        }

        @Override
        public void onStream(Publisher<ByteBuffer> publisher) {
            publisher.subscribe(new Subscriber<>() {

                @Override
                public void onSubscribe(Subscription subs) {
                }

                @Override
                public void onNext(ByteBuffer byteBuffer) {
                    stringBuilder.append(new String(byteBuffer.array()));
                }

                @Override
                public void onError(Throwable throwable) {

                }

                @Override
                public void onComplete() {

                }
            });
        }

        @Override
        public void onError(Throwable throwable) {
            this.throwableArg = throwable;
        }
    }

    private static class MockSdkHttpRequest implements SdkHttpFullRequest {


        @Override
        public String protocol() {
            return "http";
        }

        @Override
        public String host() {
            return "testHost";
        }

        @Override
        public int port() {
            return 8080;
        }

        @Override
        public String encodedPath() {
            return "";
        }

        @Override
        public Map<String, List<String>> rawQueryParameters() {
            return Collections.emptyMap();
        }

        @Override
        public SdkHttpMethod method() {
            return SdkHttpMethod.GET;
        }

        @Override
        public Map<String, List<String>> headers() {
            return Collections.singletonMap("testHeaderKey", Collections.singletonList("testHeaderValue"));
        }

        @Override
        public Builder toBuilder() {
            return null;
        }


        @Override
        public Optional<ContentStreamProvider> contentStreamProvider() {
            return Optional.of(() -> new ByteArrayInputStream("testContent".getBytes()));
        }
    }
}