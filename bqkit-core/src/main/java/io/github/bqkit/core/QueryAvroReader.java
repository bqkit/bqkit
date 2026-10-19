package io.github.bqkit.core;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
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
import org.apache.avro.NameValidator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

/** Runs a query into a destination table, then reads that table through the Storage Read API in Avro. */
public class QueryAvroReader {

    private final BigQuery bigQuery;
    private final BigQueryReadClient readClient;

    public QueryAvroReader(BigQuery bigQuery, BigQueryReadClient readClient) {
        this.bigQuery = bigQuery;
        this.readClient = readClient;
    }

    public List<GenericRecord> query(String sql) {
        return run(QueryJobConfiguration.newBuilder(sql).build());
    }

    /** Writes the result to {@code destination} (truncated first) instead of the anonymous result table. */
    public List<GenericRecord> query(String sql, TableId destination) {
        return run(QueryJobConfiguration.newBuilder(sql)
                .setDestinationTable(destination)
                .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
                .build());
    }

    private List<GenericRecord> run(QueryJobConfiguration configuration) {
        try {
            Job job = bigQuery.create(JobInfo.of(configuration)).waitFor();
            if (Objects.isNull(job)) {
                throw new IllegalStateException("BigQuery job not found");
            }
            if (Objects.nonNull(job.getStatus().getError())) {
                throw new IllegalStateException("BigQuery job failed: " + job.getStatus().getError());
            }
            TableId destination = ((QueryJobConfiguration) job.getConfiguration()).getDestinationTable();
            if (Objects.isNull(destination)) {
                throw new IllegalStateException("BigQuery job has no destination table");
            }
            return readTable(destination);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    public List<GenericRecord> readTable(TableId table) {
        String project = Objects.requireNonNullElse(table.getProject(), bigQuery.getOptions().getProjectId());
        ReadSession session = readClient.createReadSession(CreateReadSessionRequest.newBuilder()
                .setParent("projects/" + project)
                .setReadSession(ReadSession.newBuilder()
                        .setTable("projects/%s/datasets/%s/tables/%s".formatted(project, table.getDataset(), table.getTable()))
                        .setDataFormat(DataFormat.AVRO))
                .setMaxStreamCount(1)
                .build());
        Schema schema = new Schema.Parser(NameValidator.NO_VALIDATION).parse(session.getAvroSchema().getSchema());
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        List<GenericRecord> records = new ArrayList<>();
        for (ReadStream stream : session.getStreamsList()) {
            ReadRowsRequest request = ReadRowsRequest.newBuilder().setReadStream(stream.getName()).build();
            for (ReadRowsResponse response : readClient.readRowsCallable().call(request)) {
                decode(response, datumReader, records);
            }
        }
        return records;
    }

    private static void decode(ReadRowsResponse response, GenericDatumReader<GenericRecord> datumReader, List<GenericRecord> into) {
        BinaryDecoder decoder = DecoderFactory.get()
                .binaryDecoder(response.getAvroRows().getSerializedBinaryRows().toByteArray(), null);
        try {
            while (!decoder.isEnd()) {
                into.add(datumReader.read(null, decoder));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
