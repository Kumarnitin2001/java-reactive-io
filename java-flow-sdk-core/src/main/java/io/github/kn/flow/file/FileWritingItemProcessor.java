package io.github.kn.flow.file;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.github.kn.flow.CompletionStageItemProcessor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * This processes input bytes by writing them to the configured file path.
 */
public class FileWritingItemProcessor implements CompletionStageItemProcessor<List<ByteBuffer>, Void> {

    private static final Logger LOG = LogManager.getLogger("FileWritingItemProcessor");
    private final AtomicLong position = new AtomicLong(0);
    private final Supplier<AsynchronousFileChannel> fileChannelSupplier;
    private volatile AsynchronousFileChannel fileChannel;

    public FileWritingItemProcessor(final Path filePath) {
        this(() -> {
            try {
                return AsynchronousFileChannel
                        .open(filePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            } catch (IOException e) {
                LOG.error("Error creating file on path:" + filePath, e);
                throw new IllegalArgumentException(e);
            }
        });
    }

    FileWritingItemProcessor(final Supplier<AsynchronousFileChannel> channelSupplier) {
        this.fileChannelSupplier = channelSupplier;
    }


    @Override
    public CompletableFuture<Void> prepare() {
        this.fileChannel = fileChannelSupplier.get();
        position.set(0);
        return CompletableFuture.allOf();
    }

    @Override
    public CompletionStage<Void> onNext(final List<ByteBuffer> buffers) {
        LOG.trace("onNext invoked args :{}", buffers);
        List<CompletionStage<Void>> writeCompletions = new ArrayList<>(buffers.size());
        buffers.stream().filter(ByteBuffer::hasRemaining)
                .forEach(b -> this.fileChannel.write(b, position
                                .getAndAdd(b.remaining()), null,
                        new CompletionHandler<>() {
                            private final CompletableFuture<Void> completion = new CompletableFuture<>();

                            {
                                writeCompletions.add(completion);
                            }

                            @Override
                            public void completed(Integer result, Object attachment) {
                                completion.complete(null);
                            }

                            @Override
                            public void failed(Throwable exc, Object attachment) {
                                completion.completeExceptionally(exc);
                            }
                        }));

        return CompletableFuture
                .allOf(writeCompletions.toArray(new CompletableFuture[writeCompletions.size()]));
    }
}
