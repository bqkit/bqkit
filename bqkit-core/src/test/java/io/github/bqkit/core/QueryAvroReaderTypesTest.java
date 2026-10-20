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

    private static final String BIGQUERY_STRING_TYPES_RECORD = """
            {"type": "record", "name": "__root__", "fields": [
              {"name": "created", "type": ["null", {"type": "string", "logicalType": "datetime"}]},
              {"name": "payload", "type": ["null", {"type": "string", "sqlType": "JSON"}]},
              {"name": "place", "type": ["null", {"type": "string", "logicalType": "geography_wkt"}]}
            ]}""";

    private static final String BIGQUERY_NESTED_STRUCT_RECORD = """
            {"type": "record", "name": "__root__", "fields": [
              {"name": "customer", "type": ["null", {"type": "record", "name": "customer", "fields": [
                {"name": "name", "type": ["null", "string"]},
                {"name": "address", "type": ["null", {"type": "record", "name": "address", "fields": [
                  {"name": "city", "type": ["null", "string"]},
                  {"name": "since", "type": ["null", {"type": "int", "logicalType": "date"}]}
                ]}]},
                {"name": "phones", "type": {"type": "array", "items": {"type": "record", "name": "phone", "fields": [
                  {"name": "number", "type": ["null", "string"]},
                  {"name": "seen", "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]}
                ]}}}
              ]}]}
            ]}""";

    private static final Schema STRING_TYPES_SCHEMA = new Schema.Parser().parse(BIGQUERY_STRING_TYPES_RECORD);
    private static final Schema NESTED_STRUCT_SCHEMA = new Schema.Parser().parse(BIGQUERY_NESTED_STRUCT_RECORD);

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

    @Test
    void reads_datetime_json_and_geography_as_strings() {
        GenericRecord row = readOne(STRING_TYPES_SCHEMA, encode(STRING_TYPES_SCHEMA, record -> {
            record.put("created", "2026-10-20T08:30:00.123456");
            record.put("payload", "{\"a\":[1,2,{\"b\":null}],\"é\":\"x\"}");
            record.put("place", "POINT(2.3522 48.8566)");
        }));

        assertThat(row.get("created")).isEqualTo("2026-10-20T08:30:00.123456");
        assertThat(row.get("payload")).isEqualTo("{\"a\":[1,2,{\"b\":null}],\"é\":\"x\"}");
        assertThat(row.get("place")).isEqualTo("POINT(2.3522 48.8566)");
    }

    @Test
    void keeps_null_datetime_json_and_geography_null() {
        GenericRecord row = readOne(STRING_TYPES_SCHEMA, encode(STRING_TYPES_SCHEMA, record -> { }));

        assertThat(row.get("created")).isNull();
        assertThat(row.get("payload")).isNull();
        assertThat(row.get("place")).isNull();
    }

    @Test
    void reads_struct_nested_in_struct_with_java_types() {
        Schema addressSchema = addressSchema();

        GenericRecord row = readOne(NESTED_STRUCT_SCHEMA, encode(NESTED_STRUCT_SCHEMA, record -> {
            GenericRecord address = new GenericData.Record(addressSchema);
            address.put("city", "Paris");
            address.put("since", (int) DAY.toEpochDay());
            GenericRecord customer = new GenericData.Record(customerSchema());
            customer.put("name", "Ada");
            customer.put("address", address);
            customer.put("phones", List.of());
            record.put("customer", customer);
        }));

        GenericRecord customer = (GenericRecord) row.get("customer");
        GenericRecord address = (GenericRecord) customer.get("address");
        assertThat(customer.get("name")).isEqualTo("Ada");
        assertThat(address.get("city")).isEqualTo("Paris");
        assertThat(address.get("since")).isEqualTo(DAY);
    }

    @Test
    void reads_repeated_struct_with_java_types() {
        Schema phoneSchema = nonNull(customerSchema().getField("phones").schema().getElementType());

        GenericRecord row = readOne(NESTED_STRUCT_SCHEMA, encode(NESTED_STRUCT_SCHEMA, record -> {
            GenericRecord phone = new GenericData.Record(phoneSchema);
            phone.put("number", "0102030405");
            phone.put("seen", TS.getEpochSecond() * 1_000_000 + TS.getNano() / 1_000);
            GenericRecord otherPhone = new GenericData.Record(phoneSchema);
            GenericRecord customer = new GenericData.Record(customerSchema());
            customer.put("phones", List.of(phone, otherPhone));
            record.put("customer", customer);
        }));

        List<?> phones = (List<?>) ((GenericRecord) row.get("customer")).get("phones");
        assertThat(phones).hasSize(2);
        assertThat(((GenericRecord) phones.get(0)).get("number")).isEqualTo("0102030405");
        assertThat(((GenericRecord) phones.get(0)).get("seen")).isEqualTo(TS);
        assertThat(((GenericRecord) phones.get(1)).get("number")).isNull();
        assertThat(((GenericRecord) phones.get(1)).get("seen")).isNull();
    }

    @Test
    void keeps_null_struct_and_null_nested_struct_null() {
        GenericRecord absent = readOne(NESTED_STRUCT_SCHEMA, encode(NESTED_STRUCT_SCHEMA, record -> { }));
        GenericRecord withoutAddress = readOne(NESTED_STRUCT_SCHEMA,
                encode(NESTED_STRUCT_SCHEMA, record -> {
                    GenericRecord customer = new GenericData.Record(customerSchema());
                    customer.put("phones", List.of());
                    record.put("customer", customer);
                }));

        assertThat(absent.get("customer")).isNull();
        GenericRecord customer = (GenericRecord) withoutAddress.get("customer");
        assertThat(customer.get("name")).isNull();
        assertThat(customer.get("address")).isNull();
        assertThat((List<?>) customer.get("phones")).isEmpty();
    }

    private static Schema customerSchema() {
        return nonNull(NESTED_STRUCT_SCHEMA.getField("customer").schema());
    }

    private static Schema addressSchema() {
        return nonNull(customerSchema().getField("address").schema());
    }

    private static Schema nonNull(Schema nullable) {
        if (nullable.getType() != Schema.Type.UNION) {
            return nullable;
        }
        return nullable.getTypes().stream()
                .filter(type -> type.getType() != Schema.Type.NULL)
                .findFirst()
                .orElseThrow();
    }

    private GenericRecord readOne(ReadRowsResponse response) {
        return readOne(new Schema.Parser().parse(BIGQUERY_RECORD), response);
    }

    @SuppressWarnings("unchecked")
    private GenericRecord readOne(Schema avroSchema, ReadRowsResponse response) {
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(ReadSession.newBuilder()
                .setAvroSchema(AvroSchema.newBuilder().setSchema(avroSchema.toString()))
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
        return encode(new Schema.Parser().parse(BIGQUERY_RECORD), values);
    }

    private static ReadRowsResponse encode(Schema schema, Consumer<GenericRecord> values) {
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
