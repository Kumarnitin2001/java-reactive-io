package io.github.kn.flow.http;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import javax.net.ssl.SSLSession;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpRequestResponseStringCodecTest {


    @BeforeEach
    void setUp() {
    }

    @Test
    void toHttpRequestReturnsRepeatableBodyReader() {
        HttpRequest to = HttpRequestResponseStringCodec
                .toHttpRequest("PUT https://testUri.com HTTP/1.1\r\n" +
                        HttpRequestResponseStringCodec.CONTENT_LENGTH + ": 8\r\n\r\n" +
                        "testBody");
        Assertions.assertTrue(HttpRequestResponseStringCodec.fromHttpRequest(to).toString().endsWith("testBody"));
        Assertions.assertTrue(HttpRequestResponseStringCodec.fromHttpRequest(to).toString().endsWith("testBody"));
    }

    @Test
    void fromHttpRequest() {
        Assertions.assertEquals("PUT https://testUri.com HTTP/1.1\r\n" +
                "testHeaderName: testHeaderValue\r\n" +
                "content-length: 8\r\n\r\n" +
                "testBody", HttpRequestResponseStringCodec
                .fromHttpRequest(HttpRequest.newBuilder().uri(URI.create("https://testUri.com"))
                        .method("PUT", HttpRequest.BodyPublishers.ofString("testBody"))
                        .header("testHeaderName", "testHeaderValue").build()).toString());
    }

    @Test
    void toHttpRequestHeaders() {
        Assertions.assertEquals("{TESTHEADERNAME=[testHeaderValue]}", HttpRequestResponseStringCodec
                .toHttpRequest("PUT https://testUri.com HTTP/1.1\r\n" +
                        "testHeaderName: testHeaderValue\r\n\r\n" +
                        "testBody").headers().map().toString());
    }

    @Test
    void toHttpRequestURI() {
        Assertions.assertEquals("https://testUri.com", HttpRequestResponseStringCodec
                .toHttpRequest("PUT https://testUri.com HTTP/1.1\r\n" +
                        "testHeaderName: testHeaderValue\r\n\r\n" +
                        "testBody").uri().toString());
    }

    @Test
    void fromResponse() {
        Assertions.assertEquals("HTTP/1.1 200\r\n" +
                "HeaderKey: HeaderValue1\r\n" +
                "HeaderKey: HeaderValue2\r\n\r\n" +
                "testStringBody", HttpRequestResponseStringCodec
                .fromHttpResponse(new HttpResponse<>() {
                    @Override
                    public int statusCode() {
                        return 200;
                    }

                    @Override
                    public HttpRequest request() {
                        return null;
                    }

                    @Override
                    public Optional<HttpResponse<byte[]>> previousResponse() {
                        return Optional.empty();
                    }

                    @Override
                    public HttpHeaders headers() {
                        return HttpHeaders.of(Collections.singletonMap("HeaderKey",
                                Arrays.asList("HeaderValue1", "HeaderValue2")), (a, b) -> true);
                    }

                    @Override
                    public byte[] body() {
                        return "testStringBody".getBytes();
                    }

                    @Override
                    public Optional<SSLSession> sslSession() {
                        return Optional.empty();
                    }

                    @Override
                    public URI uri() {
                        return null;
                    }

                    @Override
                    public HttpClient.Version version() {
                        return HttpClient.Version.HTTP_1_1;
                    }
                }).toString());
    }
}