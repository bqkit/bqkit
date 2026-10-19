package io.github.bqkit.core;

import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

/** Arrow payloads shaped like the ones BigQuery sends: bare schema and record batch messages, no stream framing. */
final class ArrowMessages {

    private ArrowMessages() {
    }

    static ReadSession session() {
        return ReadSession.newBuilder()
                .setArrowSchema(ArrowSchema.newBuilder().setSerializedSchema(schema(new Schema(List.of()))))
                .build();
    }

    static ByteString recordBatch(Schema schema, String... names) {
        return serialize(schema, false, names);
    }

    static ByteString dictionaryBatch(Schema schema, String... names) {
        return serialize(schema, true, names);
    }

    private static ByteString serialize(Schema schema, boolean dictionary, String... names) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VarCharVector name = (VarCharVector) root.getVector(0);
            for (int i = 0; i < names.length; i++) {
                name.setSafe(i, names[i].getBytes(StandardCharsets.UTF_8));
            }
            root.setRowCount(names.length);
            WriteChannel channel = new WriteChannel(Channels.newChannel(out));
            try (ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch()) {
                if (dictionary) {
                    try (ArrowDictionaryBatch dictionaryBatch = new ArrowDictionaryBatch(0, batch, false)) {
                        MessageSerializer.serialize(channel, dictionaryBatch);
                    }
                } else {
                    MessageSerializer.serialize(channel, batch);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return ByteString.copyFrom(out.toByteArray());
    }

    /** A record batch claiming more rows than Arrow Java can hold, which fails to deserialize after its body is read. */
    static ByteString oversizedRecordBatch() {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        RecordBatch.startNodesVector(builder, 0);
        int nodes = builder.endVector();
        RecordBatch.startBuffersVector(builder, 0);
        int buffers = builder.endVector();
        RecordBatch.startRecordBatch(builder);
        RecordBatch.addLength(builder, Integer.MAX_VALUE + 1L);
        RecordBatch.addNodes(builder, nodes);
        RecordBatch.addBuffers(builder, buffers);
        int recordBatch = RecordBatch.endRecordBatch(builder);
        byte[] body = new byte[8];
        ByteBuffer message = MessageSerializer.serializeMessage(
                builder, MessageHeader.RecordBatch, recordBatch, body.length, IpcOption.DEFAULT);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            WriteChannel channel = new WriteChannel(Channels.newChannel(out));
            MessageSerializer.writeMessageBuffer(channel, message.remaining(), message);
            channel.write(body);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return ByteString.copyFrom(out.toByteArray());
    }

    static ByteString schema(Schema schema) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return ByteString.copyFrom(out.toByteArray());
    }
}
