package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
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
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class QueryArrowReaderReadSessionTest {

    private final BigQuery bigQuery = BigQueryClients.bigQuery(new BigQueryConnection(
            "my-project", null, null, false, NoCredentials.getInstance()));
    private final BigQueryReadClient readClient = mock(BigQueryReadClient.class);

    @Test
    void reads_a_table_of_another_project_in_that_project() {
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(ArrowMessages.session());

        new QueryArrowReader(bigQuery, readClient).readTable(TableId.of("other-project", "shop", "products"),
                batch -> { });

        ArgumentCaptor<CreateReadSessionRequest> request = ArgumentCaptor.forClass(CreateReadSessionRequest.class);
        verify(readClient).createReadSession(request.capture());
        assertThat(request.getValue().getParent()).isEqualTo("projects/other-project");
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
                .hasMessageContaining("dictionary");
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
