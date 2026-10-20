package io.github.bqkit.core;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
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

/**
 * Runs a query into a destination table, then reads that table through the Storage Read API in Avro.
 *
 * <p>Only a SELECT statement writes a table to read: a DML, DDL or script job is rejected once it has run.
 * With more than one stream, the streams are read in parallel on virtual threads, so the consumer is called
 * from several threads at once and rows come in no guaranteed order: add an {@code ORDER BY} and read one
 * stream to keep it.
 */
public final class QueryAvroReader {

    /**
     * TIMESTAMP as {@code Instant}, DATE as {@code LocalDate}, TIME as {@code LocalTime},
     * NUMERIC and BIGNUMERIC as {@code BigDecimal}.
     */
    private static final GenericData JAVA_TYPES = javaTypes();

    private final BigQuery bigQuery;
    private final BigQueryReadClient readClient;
    private final int maxStreams;

    /** Reads one stream at a time. */
    public QueryAvroReader(BigQuery bigQuery, BigQueryReadClient readClient) {
        this(bigQuery, readClient, 1);
    }

    public QueryAvroReader(BigQuery bigQuery, BigQueryReadClient readClient, int maxStreams) {
        this.bigQuery = Objects.requireNonNull(bigQuery, "bigQuery");
        this.readClient = Objects.requireNonNull(readClient, "readClient");
        this.maxStreams = ReadSessions.requirePositive(maxStreams);
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
     * Runs {@code configuration} as is, for query parameters, {@code maximumBytesBilled} or labels,
     * then reads the table the job wrote to. The job runs in the location of the client, set with
     * {@link BigQueryConnection#withLocation(String)}.
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
        ReadSession session = ReadSessions.create(bigQuery, readClient, table, DataFormat.AVRO, maxStreams);
        Schema schema = new Schema.Parser().parse(session.getAvroSchema().getSchema());
        readStringsAsString(schema);
        try {
            ReadStreams.read(readClient, session.getStreamsList(), rows, (responses, consumer) -> {
                GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema, schema, JAVA_TYPES);
                for (ReadRowsResponse response : responses) {
                    decode(response, datumReader, consumer);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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

    /** Reads the payload through a stream, without first copying it into one array. */
    private static void decode(ReadRowsResponse response, GenericDatumReader<GenericRecord> datumReader,
                               Consumer<GenericRecord> rows) throws IOException {
        BinaryDecoder decoder = DecoderFactory.get()
                .binaryDecoder(response.getAvroRows().getSerializedBinaryRows().newInput(), null);
        while (!decoder.isEnd()) {
            rows.accept(datumReader.read(null, decoder));
        }
    }
}
