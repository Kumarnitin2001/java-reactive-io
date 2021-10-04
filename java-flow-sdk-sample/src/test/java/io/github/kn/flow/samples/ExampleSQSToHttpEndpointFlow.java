package io.github.kn.flow.samples;

import io.github.kn.flow.CompletionStageItemProcessor;
import io.github.kn.flow.CompletionSubscriberImpl;
import io.github.kn.flow.Optionals;
import io.github.kn.flow.aws.sfn.SfnInitiatingProcessor;
import io.github.kn.flow.aws.sqs.SQSMessagePollingPublisher;
import io.github.kn.flow.aws.sqs.SQSMessagePublishingSubscriberBuilder;
import io.github.kn.flow.aws.sqs.SQSPublishingItemProcessor;
import io.github.kn.flow.aws.sqs.SQSReceivedMessageResponseProcessor;
import io.github.kn.flow.http.HttpRequestItemProcessor;
import io.github.kn.flow.http.HttpRequestResponseStringCodec;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

/**
 * A Sample implementation of SQS to SQS processing pipeline.<br>
 * This pipeline expects to receive HTTP request invocations as SQS Messages wrap an inbound SQS queue.
 * The ({@link Message#body()}) must contain the HTTP request encoded in string format
 * as per <a href="https://tools.ietf.org/html/rfc2616#section-5"> rfc2616#section-5</a> <br>
 * The processing of the request message simply executes the HTTP request on an as-is basis, completing the
 * flow with the Http response being published out as an SQS Message to an outbound Queue.
 * The ({@link SendMessageRequest#messageBody()}) contains the HTTP response encoded in string format
 * as per <a href="https://tools.ietf.org/html/rfc2616#section-6"> rfc2616#section-6</a>
 * <br>
 * In addition to publishing the HTTPResponse as SQS message, this flow also starts executing an AWS-Sfn
 * state machine, in case of HTTP error responses as per
 * <a href="https://tools.ietf.org/html/rfc2616#section-10"> rfc2616#section-10</a>.
 * <br>
 * <i>Note that SQS inbount and out-bound queue names and also the AWS-Sfn State Machine ARN is configured as a
 * constant in the code. The AWS credentials
 * (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) and region (AWS_DEFAULT_REGION) values need to be
 * provided externally - as java environment variables - to composeAsync this example</i>
 * <br>
 *
 * @see SfnInitiatingProcessor
 * @see SQSPublishingItemProcessor
 * @see HttpRequestItemProcessor
 * @see <a href="https://tools.ietf.org/html/rfc2616">https://tools.ietf.org/html/rfc2616.</a>
 */
public class ExampleSQSToHttpEndpointFlow {
    public static final String INPUT_QUEUE_NAME = SQSMessagePublishingSubscriberBuilder.DEFAULT_QUEUE_NAME;
    public static final String OUTPUT_QUEUE_NAME = SQSMessagePublishingSubscriberBuilder.DEFAULT_QUEUE_NAME + "-output";
    public static final String STATE_MACHINE_ARN = "arn:aws:states:ap-south-1:576167309617:stateMachine" +
            ":GenericHTTPInvoker";

    public static void main(String[] args) {
        SqsAsyncClient sqsAsyncClient = SqsAsyncClient.builder()
                .asyncConfiguration(ClientAsyncConfiguration.builder()
                        .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, ForkJoinPool
                                .commonPool())
                        .build())
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                        .readTimeout(Duration.ofSeconds(65l))
                        .writeTimeout(Duration.ofSeconds(65l))
                        .connectionTimeout(Duration.ofSeconds(65l))
                        .maxConcurrency(128)).build();

        CompletionStageItemProcessor<Optionals<String>, Optionals<?>> messageProcessorPipeline =
                CompletionStageItemProcessor
                        .aggregateProcessor(CompletionStageItemProcessor
                                .<String>completedProcessor()
                                .apply(HttpRequestResponseStringCodec::toHttpRequest)
                                .compose(HttpRequestItemProcessor.builder()
                                        .executor(ForkJoinPool.commonPool())
                                        .build()))
                        .composeAsync(sfnStarter())
                        .compose(sqsPublisher(sqsAsyncClient));

        SQSMessagePollingPublisher.builder(INPUT_QUEUE_NAME)
                .client(sqsAsyncClient)
                .setMaxPollsPerThread(1)
                .setMaxPollingThreads(1)
                .build()
                .subscribe(CompletionSubscriberImpl
                        .wrap(new SQSReceivedMessageResponseProcessor(sqsAsyncClient, INPUT_QUEUE_NAME,
                                messageProcessorPipeline), 100, 1));
    }

    private static CompletionStageItemProcessor<Optionals<HttpResponse<byte[]>>,
            Optionals<HttpResponse<byte[]>>> sqsPublisher(SqsAsyncClient sqsAsyncClient) {
        return CompletionStageItemProcessor
                .<Optionals<HttpResponse<byte[]>>>completedProcessor()
                .apply(s -> s.map(r -> HttpRequestResponseStringCodec.fromHttpResponse(r).toString()).stream())
                .compose(new SQSPublishingItemProcessor(sqsAsyncClient, OUTPUT_QUEUE_NAME))
                .apply((i, s) -> i.mergeEmpties(Optionals.<SQSPublishingItemProcessor.ResponseEntry>create(s
                        .filter(SQSPublishingItemProcessor.ResponseEntry::isSuccess).collect(Collectors.toList()))));
    }

    private static CompletionStageItemProcessor<Optionals<HttpResponse<byte[]>>,
            Optionals<HttpResponse<byte[]>>> sfnStarter() {
        return CompletionStageItemProcessor.aggregateProcessor(
                CompletionStageItemProcessor
                        .<HttpResponse<byte[]>>completedProcessor()
                        .composeAsyncOptionally(
                                r -> r.statusCode() >= 300,
                                CompletionStageItemProcessor
                                        .<HttpResponse<byte[]>>completedProcessor()
                                        .apply(r -> HttpRequestResponseStringCodec
                                                .fromHttpRequest(r
                                                        .request())
                                                .toString())
                                        .compose(SfnInitiatingProcessor.<String>builder(STATE_MACHINE_ARN)
                                                .build())
                        ));
    }
}
