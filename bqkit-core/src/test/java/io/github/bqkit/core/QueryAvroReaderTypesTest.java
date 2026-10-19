package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

/** Reads rows encoded with the Avro schema that the Storage Read API sends for BigQuery types. */
class QueryAvroReaderTypesTest {

    private static final String BIGQUERY_RECORD = """
            {"type": "record", "name": "__root__", "fields": [
              {"name": "ts", "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]},
              {"name": "day", "type": ["null", {"type": "int", "logicalType": "date"}]},
              {"name": "hour", "type": ["null", {"type": "long", "logicalType": "time-micros"}]},
              {"name": "amount", "type": ["null",
                {"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 9}]},
              {"name": "big", "type": ["null",
                {"type": "bytes", "logicalType": "decimal", "precision": 77, "scale": 38}]},
              {"name": "name", "type": ["null", "string"]},
              {"name": "tags", "type": {"type": "array", "items": "string"}}
            ]}""";

    private static final Instant TS = Instant.parse("2026-10-20T08:30:00.123456Z");
    private static final LocalDate DAY = LocalDate.of(2026, 10, 20);
    private static final LocalTime HOUR = LocalTime.of(8, 30, 0, 123_456_000);
    private static final BigDecimal AMOUNT = new BigDecimal("12.345000000");
    private static final BigDecimal BIG = new BigDecimal("1.50000000000000000000000000000000000000");

    private final BigQuery bigQuery = BigQueryClients.bigQuery(BigQueryConnection.google(
            "my-project", NoCredentials.getInstance()));
    private final BigQueryReadClient readClient = mock(BigQueryReadClient.class);

    @Test
    void reads_bigquery_types_as_java_types() {
        GenericRecord row = readOne(encode(record -> {
            record.put("ts", TS.getEpochSecond() * 1_000_000 + TS.getNano() / 1_000);
            record.put("day", (int) DAY.toEpochDay());
            record.put("hour", HOUR.toNanoOfDay() / 1_000);
            record.put("amount", ByteBuffer.wrap(AMOUNT.unscaledValue().toByteArray()));
            record.put("big", ByteBuffer.wrap(BIG.unscaledValue().toByteArray()));
            record.put("name", "apple");
            record.put("tags", List.of("red", "fruit"));
        }));

        assertThat(row.get("ts")).isEqualTo(TS);
        assertThat(row.get("day")).isEqualTo(DAY);
        assertThat(row.get("hour")).isEqualTo(HOUR);
        assertThat(row.get("amount")).isEqualTo(AMOUNT);
        assertThat(row.get("big")).isEqualTo(BIG);
        assertThat(row.get("name")).isEqualTo("apple");
        assertThat(row.get("tags")).asInstanceOf(InstanceOfAssertFactories.LIST).containsExactly("red", "fruit");
    }

    @Test
    void keeps_null_columns_null() {
        GenericRecord row = readOne(encode(record -> record.put("tags", List.of())));

        assertThat(row.get("ts")).isNull();
        assertThat(row.get("day")).isNull();
        assertThat(row.get("hour")).isNull();
        assertThat(row.get("amount")).isNull();
        assertThat(row.get("big")).isNull();
        assertThat(row.get("name")).isNull();
    }

    @SuppressWarnings("unchecked")
    private GenericRecord readOne(ReadRowsResponse response) {
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(ReadSession.newBuilder()
                .setAvroSchema(AvroSchema.newBuilder().setSchema(BIGQUERY_RECORD))
                .addStreams(ReadStream.newBuilder().setName("stream-0"))
                .build());
        ServerStreamingCallable<ReadRowsRequest, ReadRowsResponse> readRows = mock(ServerStreamingCallable.class);
        ServerStream<ReadRowsResponse> responses = mock(ServerStream.class);
        when(readClient.readRowsCallable()).thenReturn(readRows);
        when(readRows.call(any(ReadRowsRequest.class))).thenReturn(responses);
        when(responses.iterator()).thenReturn(List.of(response).iterator());

        List<GenericRecord> rows = new QueryAvroReader(bigQuery, readClient).readTable(TableId.of("shop", "typed"));

        assertThat(rows).hasSize(1);
        return rows.get(0);
    }

    private static ReadRowsResponse encode(Consumer<GenericRecord> values) {
        Schema schema = new Schema.Parser().parse(BIGQUERY_RECORD);
        GenericRecord record = new GenericData.Record(schema);
        values.accept(record);
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
