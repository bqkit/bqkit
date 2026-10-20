package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class QueryAvroReaderParallelTest {

    private static final String NAME_RECORD = """
            {"type": "record", "name": "row", "fields": [{"name": "name", "type": "string"}]}""";

    private final BigQuery bigQuery = BigQueryClients.bigQuery(BigQueryConnection.google(
            "my-project", NoCredentials.getInstance()));
    private final BigQueryReadClient readClient = mock(BigQueryReadClient.class);
    private final ReadStreamsStub streams = new ReadStreamsStub(readClient);

    @Test
    void asks_for_a_single_stream_by_default() {
        session(0);

        new QueryAvroReader(bigQuery, readClient).readTable(TableId.of("shop", "products"));

        assertThat(sessionRequest().getMaxStreamCount()).isEqualTo(1);
    }

    @Test
    void asks_for_as_many_streams_as_the_reader_reads_at_once() {
        session(0);

        new QueryAvroReader(bigQuery, readClient, 4).readTable(TableId.of("shop", "products"));

        assertThat(sessionRequest().getMaxStreamCount()).isEqualTo(4);
    }

    @Test
    void rejects_reading_less_than_one_stream_at_once() {
        assertThatThrownBy(() -> new QueryAvroReader(bigQuery, readClient, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxStreams");
    }

    @Test
    void reads_the_streams_of_the_session_at_the_same_time() {
        session(2);
        CountDownLatch bothOpen = new CountDownLatch(2);
        streams.stream("stream-0", () -> ReadStreamsStub.await(bothOpen), row("apple"));
        streams.stream("stream-1", () -> ReadStreamsStub.await(bothOpen), row("banana"), row("cherry"));

        List<GenericRecord> records = new QueryAvroReader(bigQuery, readClient, 2)
                .readTable(TableId.of("shop", "products"));

        assertThat(records).extracting(record -> record.get("name"))
                .containsExactlyInAnyOrder("apple", "banana", "cherry");
    }

    @Test
    void hands_the_rows_to_the_consumer_one_at_a_time() {
        session(2);
        CountDownLatch bothOpen = new CountDownLatch(2);
        streams.stream("stream-0", () -> ReadStreamsStub.await(bothOpen), row("apple"));
        streams.stream("stream-1", () -> ReadStreamsStub.await(bothOpen), row("banana"));
        AtomicInteger inside = new AtomicInteger();
        AtomicBoolean overlapped = new AtomicBoolean();
        CountDownLatch bothCalled = new CountDownLatch(2);

        new QueryAvroReader(bigQuery, readClient, 2).readTable(TableId.of("shop", "products"), row -> {
            if (inside.incrementAndGet() > 1) {
                overlapped.set(true);
            }
            bothCalled.countDown();
            try {
                bothCalled.await(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            inside.decrementAndGet();
        });

        assertThat(overlapped).isFalse();
    }

    @Test
    void stops_the_other_streams_when_one_fails() {
        session(2);
        CountDownLatch otherOpen = new CountDownLatch(1);
        ReadRowsResponse truncated = ReadRowsResponse.newBuilder()
                .setAvroRows(AvroRows.newBuilder().setSerializedBinaryRows(ByteString.copyFrom(new byte[] {10, 'a'})))
                .build();
        streams.stream("stream-0", () -> ReadStreamsStub.waitFor(otherOpen), truncated);
        var other = streams.streamUntilCancelled("stream-1", otherOpen::countDown);

        assertThatThrownBy(() -> new QueryAvroReader(bigQuery, readClient, 2)
                .readTable(TableId.of("shop", "products")))
                .isInstanceOf(UncheckedIOException.class);

        verify(other, atLeastOnce()).cancel();
    }

    @Test
    void throws_the_consumer_failure_as_is() {
        session(2);
        streams.stream("stream-0", () -> { }, row("apple"));
        streams.stream("stream-1", () -> { }, row("banana"));

        assertThatThrownBy(() -> new QueryAvroReader(bigQuery, readClient, 2)
                .readTable(TableId.of("shop", "products"), row -> {
                    throw new IllegalStateException("consumer failed");
                }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("consumer failed");
    }

    private void session(int streamCount) {
        ReadSession.Builder session = ReadSession.newBuilder()
                .setAvroSchema(AvroSchema.newBuilder().setSchema(NAME_RECORD));
        for (int i = 0; i < streamCount; i++) {
            session.addStreams(ReadStream.newBuilder().setName("stream-" + i));
        }
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(session.build());
    }

    private CreateReadSessionRequest sessionRequest() {
        ArgumentCaptor<CreateReadSessionRequest> request = ArgumentCaptor.forClass(CreateReadSessionRequest.class);
        verify(readClient).createReadSession(request.capture());
        return request.getValue();
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
