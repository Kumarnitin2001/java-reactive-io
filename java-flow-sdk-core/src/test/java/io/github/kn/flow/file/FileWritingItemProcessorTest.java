package io.github.kn.flow.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 *
 */
class FileWritingItemProcessorTest {
    private FileWritingItemProcessor processor;
    private MockAsynchronousFileChannel mockAsynchronousFileChannel;

    @BeforeEach
    public void setUp() {
        mockAsynchronousFileChannel = new MockAsynchronousFileChannel();
        processor = new FileWritingItemProcessor(() -> mockAsynchronousFileChannel);
    }

    @Test
    void prepare() {
        Assertions.assertFalse(processor.prepare().isCompletedExceptionally());
    }

    @Test
    void onNextCompletionException() {
        processor.prepare();
        mockAsynchronousFileChannel.setCompletionException(new IllegalArgumentException());
        Assertions.assertTrue(processor.onNext(List.of(ByteBuffer.allocate(1))).toCompletableFuture()
                .isCompletedExceptionally());
    }

    @Test
    void onNextCompletionSuccess() {
        processor.prepare();
        Assertions.assertFalse(processor.onNext(List.of(ByteBuffer.allocate(1), ByteBuffer.allocate(2)))
                .toCompletableFuture()
                .isCompletedExceptionally());
        Assertions
                .assertEquals("[java.nio.HeapByteBuffer[pos=0 lim=1 cap=1], java.nio.HeapByteBuffer[pos=0 lim=2 " +
                        "cap=2]]", mockAsynchronousFileChannel
                        .getWrtByteBuffer().toString());
        Assertions.assertEquals(1, mockAsynchronousFileChannel.getWrtPosition());
        processor.onNext(List.of(ByteBuffer.allocate(1)));
        Assertions.assertEquals(3, mockAsynchronousFileChannel.getWrtPosition());
    }

    private class MockAsynchronousFileChannel extends AsynchronousFileChannel {

        private List<ByteBuffer> wrtByteBuffers = new ArrayList<>();
        private long wrtPosition;
        private Optional<Exception> completionException = Optional.empty();

        public List<ByteBuffer> getWrtByteBuffer() {
            return wrtByteBuffers;
        }

        public long getWrtPosition() {
            return wrtPosition;
        }

        public MockAsynchronousFileChannel setCompletionException(Exception completionException) {
            this.completionException = Optional.of(completionException);
            return this;
        }

        @Override
        public Future<FileLock> lock(long position, long size, boolean shared) {
            return null;
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return null;
        }

        @Override
        public <A> void read(ByteBuffer dst, long position, A attachment,
                             CompletionHandler<Integer, ? super A> handler) {

        }

        @Override
        public Future<Integer> read(ByteBuffer dst, long position) {
            return null;
        }

        @Override
        public <A> void write(ByteBuffer src, long position, A attachment,
                              CompletionHandler<Integer, ? super A> handler) {

            this.wrtByteBuffers.add(src);
            this.wrtPosition = position;
            completionException
                    .ifPresentOrElse((e) -> handler.failed(e, attachment), () -> handler.completed(1, attachment));
        }

        @Override
        public Future<Integer> write(ByteBuffer src, long position) {
            return null;
        }

        @Override
        public void force(boolean metaData) throws IOException {

        }

        @Override
        public <A> void lock(long position, long size, boolean shared, A attachment, CompletionHandler<FileLock, ?
                super A> handler) {

        }

        @Override
        public AsynchronousFileChannel truncate(long size) throws IOException {
            return null;
        }

        @Override
        public long size() throws IOException {
            return 0;
        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public void close() throws IOException {

        }
    }
}