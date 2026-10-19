package io.github.bqkit.core;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.BigQuery;
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
        return readTable(QueryJobs.run(bigQuery, QueryJobs.anonymous(sql)));
    }

    /** Writes the result to {@code destination} (truncated first) instead of the anonymous result table. */
    public List<GenericRecord> query(String sql, TableId destination) {
        return readTable(QueryJobs.run(bigQuery, QueryJobs.into(sql, destination)));
    }

    public List<GenericRecord> readTable(TableId table) {
        ReadSession session = ReadSessions.create(bigQuery, readClient, table, DataFormat.AVRO);
        Schema schema = new Schema.Parser().parse(session.getAvroSchema().getSchema());
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        List<GenericRecord> records = new ArrayList<>();
        for (ReadStream stream : session.getStreamsList()) {
            ReadRowsRequest request = ReadRowsRequest.newBuilder().setReadStream(stream.getName()).build();
            ServerStream<ReadRowsResponse> responses = readClient.readRowsCallable().call(request);
            try {
                for (ReadRowsResponse response : responses) {
                    decode(response, datumReader, records);
                }
            } finally {
                responses.cancel();
            }
        }
        return records;
    }

    private static void decode(ReadRowsResponse response, GenericDatumReader<GenericRecord> datumReader,
                               List<GenericRecord> into) {
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
