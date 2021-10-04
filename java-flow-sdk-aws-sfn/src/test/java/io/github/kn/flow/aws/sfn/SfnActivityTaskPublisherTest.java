package io.github.kn.flow.aws.sfn;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.kn.flow.util.MockCompletionSubscriber;
import io.github.kn.flow.util.MockRunnable;
import io.github.kn.flow.util.MockSubmissionPublisher;
import io.github.kn.flow.util.MockSubscription;
import software.amazon.awssdk.services.sfn.SfnAsyncClient;
import software.amazon.awssdk.services.sfn.model.GetActivityTaskRequest;
import software.amazon.awssdk.services.sfn.model.GetActivityTaskResponse;

/**
 *
 */
class SfnActivityTaskPublisherTest {

	private SfnActivityTaskPublisher publisher;
	private MockSubmissionPublisher<GetActivityTaskResponse> mockPublisherDelegate;
	private MockCompletionSubscriber<GetActivityTaskResponse> mockSubscriber;
	private MockRunnable runnable;
	private MockSfnAsyncClient mockSfnAsyncClient;
	private SfnActivityTaskPublisher.SfnPoller poller;

	@BeforeEach
	void setUp() {
		mockSubscriber = new MockCompletionSubscriber<>();
		runnable = new MockRunnable();
		mockPublisherDelegate = new MockSubmissionPublisher<>((r) -> r.run(), 1);
		publisher = new SfnActivityTaskPublisher(() -> runnable, (r) -> r.run(), 1, mockPublisherDelegate);
		mockSfnAsyncClient = new MockSfnAsyncClient();
		poller = new SfnActivityTaskPublisher.SfnPoller(mockSfnAsyncClient, "testArn", 1, mockPublisherDelegate);
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
		SfnActivityTaskPublisher publisher = new SfnActivityTaskPublisher(() -> mockRunnable, (r) -> r.run(), 2,
				mockPublisherDelegate);
		publisher.subscribe(mockSubscriber);
		Assertions.assertEquals(2, mockRunnable.getTimesRunInvoked());
	}

	@Test
	void pollerWithPollCountOne() {
		poller.run();
		Assertions.assertEquals(1, mockSubscriber.getItemsList().size());
		Assertions.assertEquals("testToken", mockSubscriber.getItemsList().get(0).taskToken());
		Assertions.assertEquals("testInput", mockSubscriber.getItemsList().get(0).input());
	}

	@Test
	void pollerWithPollCountTwo() {
		mockPublisherDelegate.setMaxRunCount(2);
		new SfnActivityTaskPublisher.SfnPoller(mockSfnAsyncClient, "testArn", 2, mockPublisherDelegate).run();
		Assertions.assertEquals(2, mockSubscriber.getItemsList().size());
	}

	void withNullTokenTask() {
		LinkedBlockingQueue<GetActivityTaskResponse> submittedTasks = new LinkedBlockingQueue<>();
		submittedTasks.add(GetActivityTaskResponse.builder().input("testInput").taskToken(null).build());
		new SfnActivityTaskPublisher.SfnPoller(mockSfnAsyncClient, "testArn", 1, mockPublisherDelegate, submittedTasks)
				.submitTasks();
		Assertions.assertEquals(0, mockSubscriber.getItemsList().size());
	}

	@Test
	void withNullTask() {
		LinkedBlockingQueue<GetActivityTaskResponse> submittedTasks = new LinkedBlockingQueue<>();
		new SfnActivityTaskPublisher.SfnPoller(mockSfnAsyncClient, "testArn", 1, mockPublisherDelegate, submittedTasks)
				.submitTasks();
		Assertions.assertEquals(0, mockSubscriber.getItemsList().size());
	}

	@Test
	void withMultipleTasks() {
		LinkedBlockingQueue<GetActivityTaskResponse> submittedTasks = new LinkedBlockingQueue<>();
		submittedTasks.add(GetActivityTaskResponse.builder().input("testInput").taskToken("testToken").build());
		submittedTasks.add(GetActivityTaskResponse.builder().input("testInput").taskToken("testToken").build());
		new SfnActivityTaskPublisher.SfnPoller(mockSfnAsyncClient, "testArn", 1, mockPublisherDelegate, submittedTasks)
				.submitTasks();
		Assertions.assertEquals(2, mockSubscriber.getItemsList().size());
	}

	@Test
	void builder() {
		Assertions.assertNotNull(SfnActivityTaskPublisher.builder("arn").setMaxPollingThreads(1).setMaxPollsPerThread(1)
				.client(new MockSfnAsyncClient()).setBufferCapacity(1).build());
	}

	private class MockSfnAsyncClient implements SfnAsyncClient {

		private GetActivityTaskRequest getActivityTaskArg;
		private CompletableFuture<GetActivityTaskResponse> activityTaskCompletableFuture = CompletableFuture
				.completedFuture(GetActivityTaskResponse.builder().input("testInput").taskToken("testToken").build());

		public GetActivityTaskRequest getGetActivityTaskArg() {
			return getActivityTaskArg;
		}

		public MockSfnAsyncClient setActivityTaskCompletableFuture(
				CompletableFuture<GetActivityTaskResponse> activityTaskCompletableFuture) {
			this.activityTaskCompletableFuture = activityTaskCompletableFuture;
			return this;
		}

		@Override
		public String serviceName() {
			return null;
		}

		@Override
		public CompletableFuture<GetActivityTaskResponse> getActivityTask(
				GetActivityTaskRequest getActivityTaskRequest) {
			this.getActivityTaskArg = getActivityTaskRequest;
			return this.activityTaskCompletableFuture;
		}

		@Override
		public void close() {

		}
	}
}