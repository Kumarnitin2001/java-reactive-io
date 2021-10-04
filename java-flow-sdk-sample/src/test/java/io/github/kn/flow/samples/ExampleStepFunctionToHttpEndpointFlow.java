package io.github.kn.flow.samples;

import io.github.kn.flow.CompletionStageItemProcessor;
import io.github.kn.flow.CompletionSubscriberImpl;
import io.github.kn.flow.aws.sfn.SfnActivityTaskProcessor;
import io.github.kn.flow.aws.sfn.SfnActivityTaskPublisher;
import io.github.kn.flow.http.HttpRequestItemProcessor;
import io.github.kn.flow.http.HttpRequestResponseStringCodec;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.sfn.SfnAsyncClient;
import software.amazon.awssdk.services.sfn.model.GetActivityTaskResponse;
import software.amazon.awssdk.services.sfn.model.SendTaskSuccessRequest;

import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

/**
 * A Sample implementation of AWS-Sfn Activity processing flow pipeline.<br>
 * This pipeline expects to receive HTTP request invocations as AWS-Sfn processing task input.
 * The ({@link GetActivityTaskResponse#input()}) must contain JSON with the HTTP request encoded in string format
 * as per <a href="https://tools.ietf.org/html/rfc2616#section-5"> rfc2616#section-5</a>. More specifically as {
 * "input": "encodedHTTPRequest"}. <br>
 * The processing step of the flow simply executes the HTTP request on an as-is basis, completing the
 * task with the HTTP response as the task output.
 * The ({@link SendTaskSuccessRequest#output()}) contains JSON with the HTTP response encoded in string format
 * as per <a href="https://tools.ietf.org/html/rfc2616#section-6"> rfc2616#section-6</a>. More specifically as {
 * "input": "encodedHTTPRequest",
 * "output":"encodedHTTPResponse"}
 * <br>
 * <br>
 * <i>Note that AWS-Sfn Activity ARN is configured as a constant in the code. The AWS credentials
 * (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) and region (AWS_DEFAULT_REGION) values need to be
 * provided externally - as java environment variables - to composeAsync this example</i>
 * <br>
 *
 * @see SfnActivityTaskPublisher
 * @see SfnActivityTaskProcessor
 */
public class ExampleStepFunctionToHttpEndpointFlow {
    public static final String ACTIVITY_ARN = "arn:aws:states:ap-south-1:576167309617:activity:invoke_http";

    public static void main(String[] args) {
        SfnAsyncClient sfnAsyncClient = SfnAsyncClient.builder()
                .asyncConfiguration(ClientAsyncConfiguration.builder()
                        .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, ForkJoinPool
                                .commonPool())
                        .build())
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                        .readTimeout(Duration.ofSeconds(65l))
                        .writeTimeout(Duration.ofSeconds(65l))
                        .connectionTimeout(Duration.ofSeconds(65l))
                        .maxConcurrency(128)).build();

        CompletionStageItemProcessor<String, String> httpProcessor =
                ((CompletionStageItemProcessor<String, HttpRequest>) item -> CompletableFuture
                        .completedStage(HttpRequestResponseStringCodec.toHttpRequest(item)))
                        .compose(HttpRequestItemProcessor.builder()
                                .executor(ForkJoinPool.commonPool())
                                .build())
                        .compose(r -> CompletableFuture
                                .completedStage(HttpRequestResponseStringCodec.fromHttpResponse(r).toString()));

        SfnActivityTaskPublisher.builder(ACTIVITY_ARN)
                .client(sfnAsyncClient)
                .setMaxPollsPerThread(100)
                .setMaxPollingThreads(1)
                .build()
                .subscribe(CompletionSubscriberImpl
                        .wrap(SfnActivityTaskProcessor
                                .builder(httpProcessor)
                                .client(sfnAsyncClient).build(), 100, 1));
    }

//    public static void main(String[] args) {
//        HttpClient javaHttpClient = HttpClient
//                .newHttpClient();
//
//        SqsAsyncClient client = SqsAsyncClient.builder().asyncConfiguration(ClientAsyncConfiguration.builder()
//                .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, ForkJoinPool.commonPool())
//                .build())
//                .httpClient(new JavaNativeSdkHttpClient(new MockHttpClient() {
//                    @Override
//                    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
//                                                                            HttpResponse.BodyHandler<T>
//                                                                                    responseBodyHandler) {
//                        HttpRequestResponseStringCodec codec = HttpRequestResponseStringCodec.fromHttpRequest
//                        (request);
//                        LOG.trace("Raw Request String {}", codec);
//                        HttpRequest decodedRequest = HttpRequestResponseStringCodec
//                                .fromHttpRequest(codec.toString());
//                        return javaHttpClient.sendAsync(decodedRequest, responseBodyHandler);
//                    }
//                })).build();
//
//        try {
//            client.getQueueUrl(GetQueueUrlRequest.builder()
//                    .queueName(SQSMessagePublishingSubscriberBuilder.DEFAULT_QUEUE_NAME)
//                    .build()).thenApply(r -> r.queueUrl())
//                    .thenApply(u -> {
//                        LOG.info("Queue URL fetched {}", u);
//                        return u;
//                    })
//                    .compose(u -> client.sendMessage(SendMessageRequest.builder().queueUrl(u)
//                            .messageBody("testMessage").build()))
//                    .exceptionally(e -> {
//                        e.printStackTrace(System.out);
//                        return null;
//                    }).get();
//        } catch (InterruptedException | ExecutionException e) {
//            e.printStackTrace();
//        }
//    }
}
