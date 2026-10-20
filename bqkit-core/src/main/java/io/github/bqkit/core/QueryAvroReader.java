package io.github.bqkit.core;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

/** Runs a query into a destination table, then reads that table through the Storage Read API in Avro. */
public class QueryAvroReader {

    /**
     * TIMESTAMP as {@code Instant}, DATE as {@code LocalDate}, TIME as {@code LocalTime},
     * NUMERIC and BIGNUMERIC as {@code BigDecimal}.
     */
    private static final GenericData JAVA_TYPES = javaTypes();

    private final BigQuery bigQuery;
    private final BigQueryReadClient readClient;

    public QueryAvroReader(BigQuery bigQuery, BigQueryReadClient readClient) {
        this.bigQuery = bigQuery;
        this.readClient = readClient;
    }

    public List<GenericRecord> query(String sql) {
        return readTable(QueryJobs.run(bigQuery, QueryJobs.anonymous(sql)));
    }

    /** Writes the result to {@code destination} (truncated first) instead of the anonymous result table. */
    public List<GenericRecord> query(String sql, TableId destination) {
        return readTable(QueryJobs.run(bigQuery, QueryJobs.into(sql, destination)));
    }

    /** Streams the anonymous result table to {@code rows}, as {@link #readTable(TableId, Consumer)} does. */
    public void query(String sql, Consumer<GenericRecord> rows) {
        Objects.requireNonNull(rows, "rows");
        readTable(QueryJobs.run(bigQuery, QueryJobs.anonymous(sql)), rows);
    }

    /** Writes the result to {@code destination} (truncated first) instead of the anonymous result table. */
    public void query(String sql, TableId destination, Consumer<GenericRecord> rows) {
        Objects.requireNonNull(rows, "rows");
        readTable(QueryJobs.run(bigQuery, QueryJobs.into(sql, destination)), rows);
    }

    /**
     * Runs {@code configuration} as is, for query parameters, {@code maximumBytesBilled}, labels or location,
     * then reads the table the job wrote to.
     */
    public List<GenericRecord> query(QueryJobConfiguration configuration) {
        Objects.requireNonNull(configuration, "configuration");
        return readTable(QueryJobs.run(bigQuery, configuration));
    }

    /** Streams the result of {@code configuration}, run as is, to {@code rows}. */
    public void query(QueryJobConfiguration configuration, Consumer<GenericRecord> rows) {
        Objects.requireNonNull(configuration, "configuration");
        Objects.requireNonNull(rows, "rows");
        readTable(QueryJobs.run(bigQuery, configuration), rows);
    }

    /** Keeps every row in memory: stream large tables with {@link #readTable(TableId, Consumer)}. */
    public List<GenericRecord> readTable(TableId table) {
        List<GenericRecord> records = new ArrayList<>();
        readTable(table, records::add);
        return records;
    }

    /**
     * Hands each row to {@code rows} as it is decoded, without keeping it.
     * If a row cannot be decoded or {@code rows} throws, the read stops: the rows before it have already
     * been handed over.
     */
    public void readTable(TableId table, Consumer<GenericRecord> rows) {
        Objects.requireNonNull(rows, "rows");
        ReadSession session = ReadSessions.create(bigQuery, readClient, table, DataFormat.AVRO);
        Schema schema = new Schema.Parser().parse(session.getAvroSchema().getSchema());
        readStringsAsString(schema);
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema, schema, JAVA_TYPES);
        for (ReadStream stream : session.getStreamsList()) {
            ReadRowsRequest request = ReadRowsRequest.newBuilder().setReadStream(stream.getName()).build();
            ServerStream<ReadRowsResponse> responses = readClient.readRowsCallable().call(request);
            try {
                for (ReadRowsResponse response : responses) {
                    decode(response, datumReader, rows);
                }
            } catch (RuntimeException | Error failure) {
                ReadSessions.cancel(responses, failure);
                throw failure;
            }
            responses.cancel();
        }
    }

    private static GenericData javaTypes() {
        GenericData data = new GenericData();
        data.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        data.addLogicalTypeConversion(new TimeConversions.DateConversion());
        data.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        data.addLogicalTypeConversion(new Conversions.DecimalConversion());
        return data;
    }

    /** STRING as {@code String} instead of {@code Utf8}, nested ones included: the fast reader reads it per field. */
    private static void readStringsAsString(Schema schema) {
        switch (schema.getType()) {
            case STRING -> GenericData.setStringType(schema, GenericData.StringType.String);
            case RECORD -> schema.getFields().forEach(field -> readStringsAsString(field.schema()));
            case UNION -> schema.getTypes().forEach(QueryAvroReader::readStringsAsString);
            case ARRAY -> readStringsAsString(schema.getElementType());
            default -> { }
        }
    }

    private static void decode(ReadRowsResponse response, GenericDatumReader<GenericRecord> datumReader,
                               Consumer<GenericRecord> rows) {
        BinaryDecoder decoder = DecoderFactory.get()
                .binaryDecoder(response.getAvroRows().getSerializedBinaryRows().toByteArray(), null);
        try {
            while (!decoder.isEnd()) {
                rows.accept(datumReader.read(null, decoder));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
