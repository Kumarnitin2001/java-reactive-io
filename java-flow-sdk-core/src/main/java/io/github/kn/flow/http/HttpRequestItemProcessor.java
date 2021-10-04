package io.github.kn.flow.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.github.kn.flow.CompletionStageItemProcessor;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 *
 */
public class HttpRequestItemProcessor implements CompletionStageItemProcessor<HttpRequest, HttpResponse<byte[]>> {
    private static final Logger LOG = LogManager.getLogger("HttpRequestItemProcessor");

    private final HttpClient httpClient;

    HttpRequestItemProcessor(final HttpClient client) {
        this.httpClient = client;
    }

    public static HttpRequestItemProcessor.Builder builder() {
        return new HttpRequestItemProcessor.Builder();
    }

    @Override
    public CompletionStage<HttpResponse<byte[]>> onNext(final HttpRequest item) {
        LOG.trace("onNext Item {}", item);
        try {
            return httpClient.sendAsync(item, HttpResponse.BodyHandlers.ofByteArray())
                    .whenComplete((r, e) -> Optional.ofNullable(e)
                            .ifPresentOrElse(exception -> LOG
                                    .error("Exception processing request: " + item,
                                            exception), () -> LOG
                                    .debug("Completed processing request {}", item)));
        } catch (Exception e) {
            return CompletableFuture.failedStage(new IllegalArgumentException(e));
        }

    }

    public static class Builder {
        private volatile Optional<HttpClient> javaClient = Optional.empty();
        private volatile Executor executor = ForkJoinPool.commonPool();

        public Builder client(final HttpClient client) {
            this.javaClient = Optional.of(client);
            return this;
        }

        public Builder executor(final Executor exec) {
            this.executor = exec;
            return this;
        }

        public HttpRequestItemProcessor build() {
            return new HttpRequestItemProcessor(
                    javaClient.orElseGet(() -> HttpClient.newBuilder()
                            .version(HttpClient.Version.HTTP_1_1)
                            .followRedirects(HttpClient.Redirect.NORMAL)
                            .connectTimeout(Duration.ofSeconds(200l))
                            .executor(executor)
                            .build()));
        }
    }
}