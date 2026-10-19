package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.protobuf.ByteString;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class QueryAvroReaderReadSessionTest {

    private static final String EMPTY_RECORD = """
            {"type": "record", "name": "row", "fields": []}""";

    private static final String NAME_RECORD = """
            {"type": "record", "name": "row", "fields": [{"name": "name", "type": "string"}]}""";

    private final BigQuery bigQuery = BigQueryClients.bigQuery(new BigQueryConnection(
            "my-project", null, null, false, NoCredentials.getInstance()));
    private final BigQueryReadClient readClient = mock(BigQueryReadClient.class);

    @Test
    void reads_a_table_of_another_project_in_that_project() {
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(ReadSession.newBuilder()
                .setAvroSchema(AvroSchema.newBuilder().setSchema(EMPTY_RECORD))
                .build());

        new QueryAvroReader(bigQuery, readClient).readTable(TableId.of("other-project", "shop", "products"));

        ArgumentCaptor<CreateReadSessionRequest> request = ArgumentCaptor.forClass(CreateReadSessionRequest.class);
        verify(readClient).createReadSession(request.capture());
        assertThat(request.getValue().getParent()).isEqualTo("projects/other-project");
        assertThat(request.getValue().getReadSession().getTable())
                .isEqualTo("projects/other-project/datasets/shop/tables/products");
    }

    @Test
    @SuppressWarnings("unchecked")
    void cancels_the_stream_when_a_row_cannot_be_decoded() {
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(ReadSession.newBuilder()
                .setAvroSchema(AvroSchema.newBuilder().setSchema(NAME_RECORD))
                .addStreams(ReadStream.newBuilder().setName("stream-0"))
                .build());
        ServerStreamingCallable<ReadRowsRequest, ReadRowsResponse> readRows = mock(ServerStreamingCallable.class);
        ServerStream<ReadRowsResponse> responses = mock(ServerStream.class);
        when(readClient.readRowsCallable()).thenReturn(readRows);
        when(readRows.call(any(ReadRowsRequest.class))).thenReturn(responses);
        ReadRowsResponse truncated = ReadRowsResponse.newBuilder()
                .setAvroRows(AvroRows.newBuilder().setSerializedBinaryRows(ByteString.copyFrom(new byte[] {10, 'a'})))
                .build();
        when(responses.iterator()).thenReturn(List.of(truncated).iterator());

        assertThatThrownBy(() -> new QueryAvroReader(bigQuery, readClient).readTable(TableId.of("shop", "products")))
                .isInstanceOf(RuntimeException.class);

        verify(responses).cancel();
    }
}
