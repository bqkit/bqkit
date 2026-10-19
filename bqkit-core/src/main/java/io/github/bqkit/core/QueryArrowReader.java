package io.github.bqkit.core;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.UncheckedIOException;
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
import org.apache.arrow.vector.types.pojo.Schema;

/** Reads a table through the Storage Read API in Arrow, one record batch at a time. */
public class QueryArrowReader {

    private final BigQuery bigQuery;
    private final BigQueryReadClient readClient;

    public QueryArrowReader(BigQuery bigQuery, BigQueryReadClient readClient) {
        this.bigQuery = bigQuery;
        this.readClient = readClient;
    }

    public void query(String sql, Consumer<VectorSchemaRoot> batches) {
        Objects.requireNonNull(batches, "batches");
        readTable(QueryJobs.run(bigQuery, QueryJobs.anonymous(sql)), batches);
    }

    /** Writes the result to {@code destination} (truncated first) instead of the anonymous result table. */
    public void query(String sql, TableId destination, Consumer<VectorSchemaRoot> batches) {
        Objects.requireNonNull(batches, "batches");
        readTable(QueryJobs.run(bigQuery, QueryJobs.into(sql, destination)), batches);
    }

    /**
     * Hands each non-empty record batch to {@code batches}; the batch is released once the consumer returns.
     * The same {@link VectorSchemaRoot} is reused for every batch: do not keep a reference to it, copy what you keep.
     * Dictionary-encoded batches are rejected.
     */
    public void readTable(TableId table, Consumer<VectorSchemaRoot> batches) {
        Objects.requireNonNull(batches, "batches");
        ReadSession session = ReadSessions.create(bigQuery, readClient, table, DataFormat.ARROW);
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema(session), allocator)) {
            VectorLoader loader = new VectorLoader(root);
            for (ReadStream stream : session.getStreamsList()) {
                ReadRowsRequest request = ReadRowsRequest.newBuilder().setReadStream(stream.getName()).build();
                ServerStream<ReadRowsResponse> responses = readClient.readRowsCallable().call(request);
                try {
                    for (ReadRowsResponse response : responses) {
                        decode(response, allocator, loader, root, batches);
                    }
                } catch (IOException | RuntimeException | Error failure) {
                    ReadSessions.cancel(responses, failure);
                    throw failure;
                }
                responses.cancel();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Skips the schema and end-of-stream messages that some servers, such as the emulator, add around the batch. */
    private static void decode(ReadRowsResponse response, BufferAllocator allocator, VectorLoader loader,
                               VectorSchemaRoot root, Consumer<VectorSchemaRoot> batches) throws IOException {
        MessageChannelReader messages = new MessageChannelReader(
                channel(response.getArrowRecordBatch().getSerializedRecordBatch()), allocator);
        for (MessageResult message = messages.readNext(); Objects.nonNull(message); message = messages.readNext()) {
            byte header = message.getMessage().headerType();
            if (header == MessageHeader.DictionaryBatch) {
                throw new IllegalStateException("Arrow dictionary batches are not supported");
            }
            if (header != MessageHeader.RecordBatch) {
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
    }

    private static Schema schema(ReadSession session) throws IOException {
        return MessageSerializer.deserializeSchema(
                channel(session.getArrowSchema().getSerializedSchema()));
    }

    private static ReadChannel channel(ByteString bytes) {
        return new ReadChannel(new ByteStringChannel(bytes));
    }
}
