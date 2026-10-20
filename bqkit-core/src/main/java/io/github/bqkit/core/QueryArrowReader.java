package io.github.bqkit.core;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.ArrowBuf;
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
    private final int maxStreams;

    /** Reads one stream at a time. */
    public QueryArrowReader(BigQuery bigQuery, BigQueryReadClient readClient) {
        this(bigQuery, readClient, 1);
    }

    public QueryArrowReader(BigQuery bigQuery, BigQueryReadClient readClient, int maxStreams) {
        this.bigQuery = bigQuery;
        this.readClient = readClient;
        this.maxStreams = ReadSessions.requirePositive(maxStreams);
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
     * Runs {@code configuration} as is, for query parameters, {@code maximumBytesBilled}, labels or location,
     * then streams the table the job wrote to.
     */
    public void query(QueryJobConfiguration configuration, Consumer<VectorSchemaRoot> batches) {
        Objects.requireNonNull(configuration, "configuration");
        Objects.requireNonNull(batches, "batches");
        readTable(QueryJobs.run(bigQuery, configuration), batches);
    }

    /**
     * Hands each non-empty record batch to {@code batches}; the batch is released once the consumer returns.
     * Each stream reuses one {@link VectorSchemaRoot} for all its batches: do not keep a reference to it, copy what
     * you keep.
     * Dictionary-encoded batches are rejected.
     */
    public void readTable(TableId table, Consumer<VectorSchemaRoot> batches) {
        Objects.requireNonNull(batches, "batches");
        ReadSession session = ReadSessions.create(bigQuery, readClient, table, DataFormat.ARROW, maxStreams);
        try (BufferAllocator allocator = new RootAllocator()) {
            Schema schema = schema(session);
            ReadStreams.read(readClient, session.getStreamsList(), batches, (responses, consumer) -> {
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                    VectorLoader loader = new VectorLoader(root);
                    for (ReadRowsResponse response : responses) {
                        decode(response, allocator, loader, root, consumer);
                    }
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Skips the schema and end-of-stream messages that some servers, such as the emulator, add around the batch,
     * releasing the body of any message that is not a record batch.
     */
    private static void decode(ReadRowsResponse response, BufferAllocator allocator, VectorLoader loader,
                               VectorSchemaRoot root, Consumer<VectorSchemaRoot> batches) throws IOException {
        MessageChannelReader messages = new MessageChannelReader(
                channel(response.getArrowRecordBatch().getSerializedRecordBatch()), allocator);
        for (MessageResult message = messages.readNext(); Objects.nonNull(message); message = messages.readNext()) {
            byte header = message.getMessage().headerType();
            ArrowBuf body = Objects.requireNonNullElseGet(message.getBodyBuffer(), allocator::getEmpty);
            if (header != MessageHeader.RecordBatch) {
                body.close();
                if (header == MessageHeader.DictionaryBatch) {
                    throw new IllegalStateException("Arrow dictionary batches are not supported");
                }
                continue;
            }
            try (ArrowRecordBatch batch = deserialize(message, body)) {
                loader.load(batch);
            }
            if (root.getRowCount() > 0) {
                batches.accept(root);
            }
            root.clear();
        }
    }

    /** Releases the body when the record batch cannot be built, as Arrow only releases it once the batch exists. */
    private static ArrowRecordBatch deserialize(MessageResult message, ArrowBuf body) throws IOException {
        try {
            return MessageSerializer.deserializeRecordBatch(message.getMessage(), body);
        } catch (IOException | RuntimeException | Error failure) {
            body.close();
            throw failure;
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
