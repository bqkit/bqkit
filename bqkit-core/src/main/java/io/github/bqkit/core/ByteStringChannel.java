package io.github.bqkit.core;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;

/** Reads a {@link ByteString} through read-only views of its chunks, without copying it into an array first. */
final class ByteStringChannel implements ReadableByteChannel {

    private final Iterator<ByteBuffer> chunks;
    private ByteBuffer current = ByteBuffer.allocate(0);
    private boolean open = true;

    ByteStringChannel(ByteString bytes) {
        this.chunks = bytes.asReadOnlyByteBufferList().iterator();
    }

    @Override
    public int read(ByteBuffer destination) throws ClosedChannelException {
        if (!open) {
            throw new ClosedChannelException();
        }
        while (!current.hasRemaining() && chunks.hasNext()) {
            current = chunks.next();
        }
        if (!current.hasRemaining()) {
            return -1;
        }
        int length = Math.min(current.remaining(), destination.remaining());
        destination.put(current.slice(current.position(), length));
        current.position(current.position() + length);
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
