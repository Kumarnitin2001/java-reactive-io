package io.github.kn.flow.http;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.kn.flow.util.MockHttpClient;

/**
 *
 */
class HttpRequestItemProcessorTest {

    public static final HttpRequest HTTP_REQUEST = HttpRequest.newBuilder().method("POST", HttpRequest.BodyPublishers
            .ofString("Action=GetQueueUrl&Version=2012-11-05&QueueName=textline-publisher"))
            .uri(URI.create("https://sqs.ap-south-1.amazonaws.com"))
            .header("User-Agent", "Java").build();

    private HttpRequestItemProcessor processor;

    private MockHttpClient httpClient;

    @BeforeEach
    void setUp() {
        httpClient = new MockHttpClient().setResponseStatusCode(200);
        processor = HttpRequestItemProcessor.builder().client(httpClient).build();
    }

    @Test
    void prepare() {
    }


    @Test
    void builderDefault() {
        Assertions.assertNotNull(HttpRequestItemProcessor.builder().build());
    }

    @Test
    void builderCustom() {
        Assertions.assertNotNull(HttpRequestItemProcessor.builder().client(new MockHttpClient()).executor(r -> r.run())
                .build());
    }

    @Test
    void onNextClientCompletionException() {
        httpClient.setCompletionException(new RuntimeException());
        CompletableFuture<HttpResponse<byte[]>> future = processor.onNext(HTTP_REQUEST).toCompletableFuture();
        Assertions.assertTrue(future.isCompletedExceptionally());
        Assertions.assertThrows(ExecutionException.class, () -> future.get());
    }

    @Test
    void onNextClientRuntimeException() {
        httpClient.setRuntimeException(new RuntimeException());
        CompletableFuture<HttpResponse<byte[]>> future = processor.onNext(HTTP_REQUEST).toCompletableFuture();
        Assertions.assertTrue(future.isCompletedExceptionally());
        Assertions.assertThrows(ExecutionException.class, () -> future.get());
    }

    @Test
    void onNextInvokesHttpRequestMethod() {
        processor.onNext(HTTP_REQUEST);
        Assertions.assertEquals("POST", httpClient.getRequestArg().method());
    }

    @Test
    void onNextInvokesHttpRequestURI() {
        processor.onNext(HTTP_REQUEST);
        Assertions.assertEquals("https://sqs.ap-south-1.amazonaws.com", httpClient.getRequestArg().uri().toString());
    }

    @Test
    void onNextInvokesHttpRequestHeaders() {
        processor.onNext(HTTP_REQUEST);
        Assertions
                .assertEquals("{User-Agent=[Java]}",
                        httpClient
                                .getRequestArg().headers().map().toString());
    }

    @Test
    void onNextInvokesHttpRequestBody() {
        processor.onNext(HTTP_REQUEST);
        Assertions.assertEquals("Action=GetQueueUrl&Version=2012-11-05&QueueName=textline-publisher", httpClient
                .getRequestBody().toString());
    }
}