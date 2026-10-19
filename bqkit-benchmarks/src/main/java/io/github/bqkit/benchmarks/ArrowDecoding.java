package io.github.bqkit.benchmarks;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;

/** Ways of decoding the Arrow batch of a response; {@link Variant#COPY} is what {@code QueryArrowReader} does. */
public final class ArrowDecoding implements AutoCloseable {

    /** How the payload reaches the message reader. */
    public enum Variant {
        /** Copies the payload into a byte array read through a {@link ByteArrayInputStream}. */
        COPY,
        /** Reads the payload through {@link ByteString#newInput()}. */
        STREAM,
        /** Reads the payload through {@link ByteString#asReadOnlyByteBuffer()}. */
        BUFFER
    }

    private final BufferAllocator allocator = new RootAllocator();
    private final VectorSchemaRoot root = VectorSchemaRoot.create(Payloads.ARROW_SCHEMA, allocator);
    private final VectorLoader loader = new VectorLoader(root);

    void decode(Variant variant, ReadRowsResponse response, Consumer<VectorSchemaRoot> batches) {
        ByteString payload = response.getArrowRecordBatch().getSerializedRecordBatch();
        ReadableByteChannel channel = switch (variant) {
            case COPY -> Channels.newChannel(new ByteArrayInputStream(payload.toByteArray()));
            case STREAM -> Channels.newChannel(payload.newInput());
            case BUFFER -> new BufferChannel(payload.asReadOnlyByteBuffer());
        };
        try {
            MessageChannelReader messages = new MessageChannelReader(new ReadChannel(channel), allocator);
            for (MessageResult message = messages.readNext(); Objects.nonNull(message);
                 message = messages.readNext()) {
                if (message.getMessage().headerType() != MessageHeader.RecordBatch) {
                    continue;
                }
                try (ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(
                        message.getMessage(), message.getBodyBuffer())) {
                    loader.load(batch);
                }
                if (root.getRowCount() > 0) {
                    batches.accept(root);
                }
                root.clear();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        root.close();
        allocator.close();
    }

    /** Hands out the bytes of a buffer without copying them into an intermediate array. */
    private static final class BufferChannel implements ReadableByteChannel {

        private final ByteBuffer source;

        BufferChannel(ByteBuffer source) {
            this.source = source;
        }

        @Override
        public int read(ByteBuffer destination) {
            if (!source.hasRemaining()) {
                return -1;
            }
            int length = Math.min(source.remaining(), destination.remaining());
            destination.put(source.slice(source.position(), length));
            source.position(source.position() + length);
            return length;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {
        }
    }
}
