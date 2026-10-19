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
import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
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
    void bills_the_read_of_a_table_of_another_project_to_the_connection_project() {
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(ReadSession.newBuilder()
                .setAvroSchema(AvroSchema.newBuilder().setSchema(EMPTY_RECORD))
                .build());

        new QueryAvroReader(bigQuery, readClient).readTable(TableId.of("other-project", "shop", "products"));

        ArgumentCaptor<CreateReadSessionRequest> request = ArgumentCaptor.forClass(CreateReadSessionRequest.class);
        verify(readClient).createReadSession(request.capture());
        assertThat(request.getValue().getParent()).isEqualTo("projects/my-project");
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
                .isInstanceOf(UncheckedIOException.class);

        verify(responses).cancel();
    }

    @Test
    @SuppressWarnings("unchecked")
    void hands_each_row_to_the_consumer_before_reading_the_next_response() {
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(ReadSession.newBuilder()
                .setAvroSchema(AvroSchema.newBuilder().setSchema(NAME_RECORD))
                .addStreams(ReadStream.newBuilder().setName("stream-0"))
                .build());
        ServerStreamingCallable<ReadRowsRequest, ReadRowsResponse> readRows = mock(ServerStreamingCallable.class);
        ServerStream<ReadRowsResponse> responses = mock(ServerStream.class);
        when(readClient.readRowsCallable()).thenReturn(readRows);
        when(readRows.call(any(ReadRowsRequest.class))).thenReturn(responses);
        Iterator<ReadRowsResponse> rows = List.of(row("apple"), row("banana")).iterator();
        AtomicInteger pulled = new AtomicInteger();
        when(responses.iterator()).thenReturn(new Iterator<>() {
            @Override
            public boolean hasNext() {
                return rows.hasNext();
            }

            @Override
            public ReadRowsResponse next() {
                pulled.incrementAndGet();
                return rows.next();
            }
        });

        assertThatThrownBy(() -> new QueryAvroReader(bigQuery, readClient).readTable(TableId.of("shop", "products"),
                row -> {
                    throw new IllegalStateException("stop at " + row.get("name"));
                }))
                .hasMessage("stop at apple");

        assertThat(pulled).hasValue(1);
        verify(responses).cancel();
    }

    @Test
    @SuppressWarnings("unchecked")
    void keeps_the_consumer_failure_when_cancelling_the_stream_fails_too() {
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(ReadSession.newBuilder()
                .setAvroSchema(AvroSchema.newBuilder().setSchema(NAME_RECORD))
                .addStreams(ReadStream.newBuilder().setName("stream-0"))
                .build());
        ServerStreamingCallable<ReadRowsRequest, ReadRowsResponse> readRows = mock(ServerStreamingCallable.class);
        ServerStream<ReadRowsResponse> responses = mock(ServerStream.class);
        when(readClient.readRowsCallable()).thenReturn(readRows);
        when(readRows.call(any(ReadRowsRequest.class))).thenReturn(responses);
        when(responses.iterator()).thenReturn(List.of(row("apple")).iterator());
        doThrow(new IllegalStateException("cancel failed")).when(responses).cancel();

        assertThatThrownBy(() -> new QueryAvroReader(bigQuery, readClient).readTable(TableId.of("shop", "products"),
                row -> {
                    throw new IllegalStateException("consumer failed");
                }))
                .hasMessage("consumer failed")
                .satisfies(failure -> assertThat(failure.getSuppressed())
                        .singleElement().extracting(Throwable::getMessage).isEqualTo("cancel failed"));
    }

    private static ReadRowsResponse row(String name) {
        Schema schema = new Schema.Parser().parse(NAME_RECORD);
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", name);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
        try {
            new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return ReadRowsResponse.newBuilder()
                .setAvroRows(AvroRows.newBuilder().setSerializedBinaryRows(ByteString.copyFrom(bytes.toByteArray())))
                .build();
    }
}
