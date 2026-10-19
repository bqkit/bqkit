package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.ArrowRecordBatch;
import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

class QueryArrowReaderReadSessionTest {

    private final BigQuery bigQuery = BigQueryClients.bigQuery(BigQueryConnection.google(
            "my-project", NoCredentials.getInstance()));
    private final BigQueryReadClient readClient = mock(BigQueryReadClient.class);

    @Test
    void bills_the_read_of_a_table_of_another_project_to_the_connection_project() {
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(ArrowMessages.session());

        new QueryArrowReader(bigQuery, readClient).readTable(TableId.of("other-project", "shop", "products"),
                batch -> { });

        ArgumentCaptor<CreateReadSessionRequest> request = ArgumentCaptor.forClass(CreateReadSessionRequest.class);
        verify(readClient).createReadSession(request.capture());
        assertThat(request.getValue().getParent()).isEqualTo("projects/my-project");
        assertThat(request.getValue().getReadSession().getTable())
                .isEqualTo("projects/other-project/datasets/shop/tables/products");
    }

    @Test
    @SuppressWarnings("unchecked")
    void reads_record_batches_sent_without_their_schema() {
        Schema schema = new Schema(List.of(Field.nullable("name", ArrowType.Utf8.INSTANCE)));
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(ReadSession.newBuilder()
                .setArrowSchema(ArrowSchema.newBuilder().setSerializedSchema(ArrowMessages.schema(schema)))
                .addStreams(ReadStream.newBuilder().setName("stream-0"))
                .build());
        ServerStreamingCallable<ReadRowsRequest, ReadRowsResponse> readRows = mock(ServerStreamingCallable.class);
        ServerStream<ReadRowsResponse> responses = mock(ServerStream.class);
        when(readClient.readRowsCallable()).thenReturn(readRows);
        when(readRows.call(any(ReadRowsRequest.class))).thenReturn(responses);
        when(responses.iterator()).thenReturn(List.of(
                rows(ArrowMessages.recordBatch(schema, "apple", "banana")),
                rows(ArrowMessages.recordBatch(schema, "cherry"))).iterator());
        List<String> names = new ArrayList<>();

        new QueryArrowReader(bigQuery, readClient).readTable(TableId.of("shop", "products"), batch -> {
            VarCharVector name = (VarCharVector) batch.getVector("name");
            for (int i = 0; i < batch.getRowCount(); i++) {
                names.add(name.getObject(i).toString());
            }
        });

        assertThat(names).containsExactly("apple", "banana", "cherry");
    }

    @Test
    void skips_the_schema_sent_before_the_record_batches() {
        Schema schema = new Schema(List.of(Field.nullable("name", ArrowType.Utf8.INSTANCE)));
        stream(schema, rows(ArrowMessages.schema(schema).concat(ArrowMessages.recordBatch(schema, "apple"))));
        List<Integer> rowCounts = new ArrayList<>();

        new QueryArrowReader(bigQuery, readClient).readTable(TableId.of("shop", "products"),
                batch -> rowCounts.add(batch.getRowCount()));

        assertThat(rowCounts).containsExactly(1);
    }

    @Test
    void bills_the_read_to_the_project_of_the_table_when_the_connection_has_no_default_project() {
        BigQuery withoutProject = mock(BigQuery.class);
        BigQueryOptions options = mock(BigQueryOptions.class);
        when(withoutProject.getOptions()).thenReturn(options);
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(ArrowMessages.session());

        new QueryArrowReader(withoutProject, readClient).readTable(TableId.of("other-project", "shop", "products"),
                batch -> { });

        ArgumentCaptor<CreateReadSessionRequest> request = ArgumentCaptor.forClass(CreateReadSessionRequest.class);
        verify(readClient).createReadSession(request.capture());
        assertThat(request.getValue().getParent()).isEqualTo("projects/other-project");
    }

    @Test
    void fails_clearly_when_neither_the_table_nor_the_connection_has_a_project() {
        BigQuery withoutProject = mock(BigQuery.class);
        when(withoutProject.getOptions()).thenReturn(mock(BigQueryOptions.class));

        assertThatThrownBy(() -> new QueryArrowReader(withoutProject, readClient)
                .readTable(TableId.of("shop", "products"), batch -> { }))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("project");
        verifyNoInteractions(readClient);
    }

    @Test
    void skips_record_batches_without_rows() throws IOException {
        Schema schema = new Schema(List.of(Field.nullable("name", ArrowType.Utf8.INSTANCE)));
        try (BufferAllocator allocator = new RootAllocator();
             MessageChannelReader messages = new MessageChannelReader(new ReadChannel(
                     new ByteStringChannel(ArrowMessages.recordBatch(schema))), allocator)) {
            assertThat(messages.readNext().getBodyBuffer()).isNull();
        }
        stream(schema, rows(ArrowMessages.recordBatch(schema)), rows(ArrowMessages.recordBatch(schema, "apple")));
        List<Integer> rowCounts = new ArrayList<>();

        new QueryArrowReader(bigQuery, readClient).readTable(TableId.of("shop", "products"),
                batch -> rowCounts.add(batch.getRowCount()));

        assertThat(rowCounts).containsExactly(1);
    }

    @Test
    void releases_the_body_of_a_record_batch_that_fails_to_deserialize() {
        Schema schema = new Schema(List.of(Field.nullable("name", ArrowType.Utf8.INSTANCE)));
        stream(schema, rows(ArrowMessages.oversizedRecordBatch()));

        assertThatThrownBy(() -> new QueryArrowReader(bigQuery, readClient).readTable(TableId.of("shop", "products"),
                batch -> { }))
                .isInstanceOf(UncheckedIOException.class)
                .satisfies(failure -> assertThat(failure.getCause().getSuppressed()).isEmpty());
    }

    @Test
    void releases_the_body_of_a_record_batch_when_deserializing_it_raises_an_error() {
        Schema schema = new Schema(List.of(Field.nullable("name", ArrowType.Utf8.INSTANCE)));
        stream(schema, rows(ArrowMessages.recordBatch(schema, "apple")));

        try (MockedStatic<MessageSerializer> serializer = mockStatic(MessageSerializer.class, CALLS_REAL_METHODS)) {
            serializer.when(() -> MessageSerializer.deserializeRecordBatch(any(Message.class), any(ArrowBuf.class)))
                    .thenThrow(new OutOfMemoryError("no memory left"));

            assertThatThrownBy(() -> new QueryArrowReader(bigQuery, readClient)
                    .readTable(TableId.of("shop", "products"), batch -> { }))
                    .isInstanceOf(OutOfMemoryError.class)
                    .satisfies(failure -> assertThat(failure.getSuppressed()).isEmpty());
        }
    }

    @Test
    void cancels_the_stream_when_the_consumer_throws() {
        Schema schema = new Schema(List.of(Field.nullable("name", ArrowType.Utf8.INSTANCE)));
        ServerStream<ReadRowsResponse> responses = stream(schema,
                rows(ArrowMessages.recordBatch(schema, "apple")), rows(ArrowMessages.recordBatch(schema, "banana")));

        assertThatThrownBy(() -> new QueryArrowReader(bigQuery, readClient).readTable(TableId.of("shop", "products"),
                batch -> {
                    throw new IllegalStateException("consumer failed");
                }))
                .hasMessage("consumer failed");

        verify(responses).cancel();
    }

    @Test
    void keeps_the_consumer_failure_when_cancelling_the_stream_fails_too() {
        Schema schema = new Schema(List.of(Field.nullable("name", ArrowType.Utf8.INSTANCE)));
        ServerStream<ReadRowsResponse> responses = stream(schema, rows(ArrowMessages.recordBatch(schema, "apple")));
        doThrow(new IllegalStateException("cancel failed")).when(responses).cancel();

        assertThatThrownBy(() -> new QueryArrowReader(bigQuery, readClient).readTable(TableId.of("shop", "products"),
                batch -> {
                    throw new IllegalStateException("consumer failed");
                }))
                .hasMessage("consumer failed")
                .satisfies(failure -> assertThat(failure.getSuppressed())
                        .singleElement().extracting(Throwable::getMessage).isEqualTo("cancel failed"));
    }

    @Test
    void rejects_dictionary_batches() {
        Schema schema = new Schema(List.of(Field.nullable("name", ArrowType.Utf8.INSTANCE)));
        stream(schema, rows(ArrowMessages.dictionaryBatch(schema, "apple")));

        assertThatThrownBy(() -> new QueryArrowReader(bigQuery, readClient).readTable(TableId.of("shop", "products"),
                batch -> { }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dictionary")
                .satisfies(failure -> assertThat(failure.getSuppressed()).isEmpty());
    }

    @SuppressWarnings("unchecked")
    private ServerStream<ReadRowsResponse> stream(Schema schema, ReadRowsResponse... rows) {
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(ReadSession.newBuilder()
                .setArrowSchema(ArrowSchema.newBuilder().setSerializedSchema(ArrowMessages.schema(schema)))
                .addStreams(ReadStream.newBuilder().setName("stream-0"))
                .build());
        ServerStreamingCallable<ReadRowsRequest, ReadRowsResponse> readRows = mock(ServerStreamingCallable.class);
        ServerStream<ReadRowsResponse> responses = mock(ServerStream.class);
        when(readClient.readRowsCallable()).thenReturn(readRows);
        when(readRows.call(any(ReadRowsRequest.class))).thenReturn(responses);
        when(responses.iterator()).thenReturn(List.of(rows).iterator());
        return responses;
    }

    private static ReadRowsResponse rows(ByteString recordBatch) {
        return ReadRowsResponse.newBuilder()
                .setArrowRecordBatch(ArrowRecordBatch.newBuilder().setSerializedRecordBatch(recordBatch))
                .build();
    }
}
