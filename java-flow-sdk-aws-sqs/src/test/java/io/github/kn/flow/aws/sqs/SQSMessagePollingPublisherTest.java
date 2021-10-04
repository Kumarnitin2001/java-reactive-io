package io.github.kn.flow.aws.sqs;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.kn.flow.util.MockCompletionSubscriber;
import io.github.kn.flow.util.MockRunnable;
import io.github.kn.flow.util.MockSubmissionPublisher;
import io.github.kn.flow.util.MockSubscription;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/**
 *
 */
class SQSMessagePollingPublisherTest {
    private SQSMessagePollingPublisher publisher;
    private MockSubmissionPublisher<ReceiveMessageResponse> mockPublisherDelegate;
    private MockCompletionSubscriber<ReceiveMessageResponse> mockSubscriber;
    private MockRunnable runnable;
    private MockSQSAsyncClient mockSQSAsyncClient;
    private SQSMessagePollingPublisher.SQSPoller poller;
    private ReceiveMessageResponse receivedMessages;

    @BeforeEach
    void setUp() {
        mockSubscriber = new MockCompletionSubscriber<>();
        runnable = new MockRunnable();
        mockPublisherDelegate = new MockSubmissionPublisher<>(r -> r.run(), 1);
        publisher = new SQSMessagePollingPublisher(() -> runnable, (r) -> r.run(), 1, mockPublisherDelegate);
        mockSQSAsyncClient = new MockSQSAsyncClient();
        receivedMessages = ReceiveMessageResponse.builder()
                .messages(Message.builder().receiptHandle("1").body("testMessage").build()).build();
        mockSQSAsyncClient
                .setGetQueueUrlResponse(CompletableFuture.completedFuture(GetQueueUrlResponse.builder().build()))
                .setReceiveMessageResponse(CompletableFuture.completedFuture(receivedMessages));
        poller = new SQSMessagePollingPublisher.SQSPoller(mockSQSAsyncClient, "testArn", 1, 1,
                mockPublisherDelegate);
        publisher.subscribe(mockSubscriber);
    }

    @Test
    void subscribeInvokesDelegateSubscription() {
        Assertions.assertNotNull(mockPublisherDelegate.getSubscribeArg());
        mockPublisherDelegate.getSubscribeArg().onSubscribe(new MockSubscription());
        Assertions.assertNotNull(mockSubscriber.getSubscription());
    }

    @Test
    void onNextInvocation() {
        mockPublisherDelegate.getSubscribeArg().onNext("item");
        Assertions.assertEquals("[item]", mockSubscriber.getItemsList().toString());
    }

    @Test
    void onCompleteInvocation() {
        mockPublisherDelegate.getSubscribeArg().onComplete();
        Assertions.assertEquals(1, mockSubscriber.getTimesCompleteInvoked());
    }

    @Test
    void onErrorInvocation() {
        mockPublisherDelegate.getSubscribeArg().onError(new RuntimeException());
        Assertions.assertEquals(RuntimeException.class, mockSubscriber.getOnErrorArg().getClass());
    }

    @Test
    void subscribeInvokesPollingExecutionNumOfPollingThreadsIsOne() {
        Assertions.assertEquals(1, runnable.getTimesRunInvoked());
    }

    @Test
    void subscribeInvokesPollingExecutionNumOfPollingThreadsIsTwo() {
        MockRunnable mockRunnable = new MockRunnable();
        SQSMessagePollingPublisher publisher = new SQSMessagePollingPublisher(() -> mockRunnable, (r) -> r.run(), 2,
                mockPublisherDelegate);
        publisher.subscribe(mockSubscriber);
        Assertions.assertEquals(2, mockRunnable.getTimesRunInvoked());
    }

    @Test
    void pollerWithPollCountOne() {
        poller.run();
        Assertions.assertEquals(1, mockSubscriber.getItemsList().size());
        Assertions.assertEquals(1, mockSubscriber.getItemsList().get(0).messages().size());
        Assertions.assertEquals("testMessage", mockSubscriber.getItemsList().get(0).messages().get(0).body());
        Assertions.assertEquals("1", mockSubscriber.getItemsList().get(0).messages().get(0).receiptHandle());
    }

    @Test
    void pollerWithPollCountOneMessageCountTwo() {
        receivedMessages = ReceiveMessageResponse.builder()
                .messages(Message.builder().receiptHandle("1").body("testMessage").build(), Message.builder()
                        .receiptHandle("2").body("testMessage2").build()).build();
        mockSQSAsyncClient
                .setReceiveMessageResponse(CompletableFuture.completedFuture(receivedMessages));
        poller.run();
        Assertions.assertEquals(1, mockSubscriber.getItemsList().size());
        Assertions.assertEquals(2, mockSubscriber.getItemsList().get(0).messages().size());
    }

    @Test
    void pollerWithPollCountTwo() {
        mockPublisherDelegate.setMaxRunCount(2);
        new SQSMessagePollingPublisher.SQSPoller(mockSQSAsyncClient, "testArn", 2, 1,
                mockPublisherDelegate).run();
        Assertions.assertEquals(2, mockSubscriber.getItemsList().size());
    }

    @Test
    void withNullTask() {
        LinkedBlockingQueue<ReceiveMessageResponse> submittedTasks = new LinkedBlockingQueue<>();
        new SQSMessagePollingPublisher.SQSPoller(mockSQSAsyncClient, "testArn", 1, 1,
                mockPublisherDelegate, submittedTasks).submitTasks();
        Assertions.assertEquals(0, mockSubscriber.getItemsList().size());
    }

    @Test
    void builder() {
        Assertions.assertNotNull(SQSMessagePollingPublisher.builder("testArn").client(mockSQSAsyncClient)
                .setPollBatchSize(1).setBufferCapacity(1).setMaxPollingThreads(1).setMaxPollsPerThread(1).build());

    }

}