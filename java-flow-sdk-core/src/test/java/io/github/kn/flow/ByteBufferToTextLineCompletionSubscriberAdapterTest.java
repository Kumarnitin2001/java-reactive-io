package io.github.kn.flow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.kn.flow.util.MockCompletionSubscriber;
import io.github.kn.flow.util.MockCompletionSubscription;

/**
 *
 */
class ByteBufferToTextLineCompletionSubscriberAdapterTest {

    private ByteBufferToTextLineCompletionSubscriberAdapter adapter;
    private MockCompletionSubscriber<String> mockCompletionSubscriber;
    private MockCompletionSubscription mockCompletionSubscription;

    @BeforeEach
    void setUp() {
        mockCompletionSubscriber = new MockCompletionSubscriber<String>();
        mockCompletionSubscription = new MockCompletionSubscription();
        adapter = ByteBufferToTextLineCompletionSubscriberAdapter
                .adapt(mockCompletionSubscriber, StandardCharsets.UTF_8, null);
        adapter.onSubscribe(mockCompletionSubscription);
    }

    @Test
    void customSeparator() {
        adapter = ByteBufferToTextLineCompletionSubscriberAdapter
                .adapt(mockCompletionSubscriber, StandardCharsets.UTF_8, ";");
        adapter.onSubscribe(mockCompletionSubscription);
        adapter.onNext(Arrays
                .asList(ByteBuffer.wrap("{First Message};{Second Message};".getBytes())));
        assertLinesMatch(Arrays.asList("{First Message}", "{Second Message}"), mockCompletionSubscriber
                .getItemsList());
    }


    @Test
    void onNextMultipleInvocations() {
        adapter.onNext(Arrays
                .asList(ByteBuffer.wrap("{First Message}\n{Second Mess".getBytes())));
        adapter.onNext(Arrays
                .asList(ByteBuffer.wrap("age}\n".getBytes())));
        assertLinesMatch(Arrays.asList("{First Message}", "{Second Message}"), mockCompletionSubscriber
                .getItemsList());
    }

    @Test
    void onCompleteMarksEndOfText() {
        adapter.onNext(Arrays
                .asList(ByteBuffer.wrap("{First Message}".getBytes())));
        assertEquals(Collections.emptyList(), mockCompletionSubscriber
                .getItemsList());
        adapter.onComplete();
        assertLinesMatch(Arrays.asList("{First Message}"), mockCompletionSubscriber
                .getItemsList());
        assertEquals(1, mockCompletionSubscriber
                .getTimesCompleteInvoked());
    }

    @Test
    void onCompleteNoOnNextInvocation() {
        adapter.onComplete();
        assertEquals(1, mockCompletionSubscriber
                .getTimesCompleteInvoked());
    }

    @Test
    void onError() {
        RuntimeException e = new RuntimeException();
        adapter.onError(e);
        assertEquals(e, mockCompletionSubscriber.getOnErrorArg());
    }

    @Test
    void onSubscriptionCompletion() {
        mockCompletionSubscriber.getSubscription().onComplete();
        assertEquals(1, mockCompletionSubscription.getTimesOnCompleteInvoked());
    }

    @Test
    void onSubscriptionCancel() {
        mockCompletionSubscriber.getSubscription().cancel();
        assertEquals(1, mockCompletionSubscription.getTimesCancelInvoked());
    }

    @Test
    void onSubscriptionRequestWithChunkSizeOne() {
        mockCompletionSubscriber.getSubscription().request(1);
        assertEquals("[1]", mockCompletionSubscription.getRequestArgs().toString());
    }

    @Test
    void onSubscriptionRequestWithChunkSizeTwo() {
        mockCompletionSubscriber.getSubscription().request(2);
        assertEquals("[1]", mockCompletionSubscription.getRequestArgs().toString());
    }
}