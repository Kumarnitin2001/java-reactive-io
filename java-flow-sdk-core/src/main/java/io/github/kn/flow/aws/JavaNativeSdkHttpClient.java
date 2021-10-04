package io.github.kn.flow.aws;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

/**
 * A custom {@link SdkAsyncHttpClient} using java native {@link java.net.http.HttpClient} as the wrapped/underlying
 * HTTP client library.
 */
public class JavaNativeSdkHttpClient extends AbstractJavaNativeSdkHttpClient implements SdkAsyncHttpClient {

    private static final Logger LOG = LogManager.getLogger("JavaNativeSdkHttpClient");
    private final HttpClient javaHttpClient;

    public JavaNativeSdkHttpClient(final HttpClient client) {
        this.javaHttpClient = client;
    }

    @Override
    public CompletableFuture<Void> execute(final AsyncExecuteRequest asyncExecuteRequest) {
        LOG.trace("execute invoked for request {}", asyncExecuteRequest.request());
        return prepareByteArrayBodyHandler(javaHttpClient
                .sendAsync(adapt(asyncExecuteRequest), HttpResponse.BodyHandlers
                        .ofByteArray()), asyncExecuteRequest.responseHandler());
    }
}
