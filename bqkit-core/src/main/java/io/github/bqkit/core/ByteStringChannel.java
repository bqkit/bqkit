package io.github.bqkit.core;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;

/** Reads a {@link ByteString} through a read-only view of its bytes, without copying it into an array first. */
final class ByteStringChannel implements ReadableByteChannel {

    private final ByteBuffer source;
    private boolean open = true;

    ByteStringChannel(ByteString bytes) {
        this.source = bytes.asReadOnlyByteBuffer();
    }

    @Override
    public int read(ByteBuffer destination) throws ClosedChannelException {
        if (!open) {
            throw new ClosedChannelException();
        }
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
        return open;
    }

    @Override
    public void close() {
        open = false;
    }
}
