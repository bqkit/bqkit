package io.github.bqkit.benchmarks;

import com.google.cloud.bigquery.storage.v1.ArrowRecordBatch;
import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

/**
 * Responses shaped like the ones BigQuery sends, parsed back from their wire bytes so that their payload is a
 * {@link ByteString} as the gRPC client hands it over. Row {@code i}: {@code id = i}, {@code name = "name-i"},
 * {@code amount = i / 2}, {@code comment} null for even rows and {@code "comment-i"} otherwise.
 */
final class Payloads {

    static final Schema AVRO_SCHEMA = SchemaBuilder.record("row").fields()
            .requiredLong("id")
            .requiredString("name")
            .requiredDouble("amount")
            .optionalString("comment")
            .endRecord();

    static final org.apache.arrow.vector.types.pojo.Schema ARROW_SCHEMA =
            new org.apache.arrow.vector.types.pojo.Schema(List.of(
                    Field.notNullable("id", new ArrowType.Int(64, true)),
                    Field.notNullable("name", ArrowType.Utf8.INSTANCE),
                    Field.notNullable("amount", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    new Field("comment", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));

    private Payloads() {
    }

    static ReadRowsResponse avro(int first, int rows) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(AVRO_SCHEMA);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
            for (int i = first; i < first + rows; i++) {
                GenericRecord row = new GenericData.Record(AVRO_SCHEMA);
                row.put("id", (long) i);
                row.put("name", name(i));
                row.put("amount", i / 2.0);
                row.put("comment", comment(i));
                writer.write(row, encoder);
            }
            encoder.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return reparse(ReadRowsResponse.newBuilder()
                .setRowCount(rows)
                .setAvroRows(AvroRows.newBuilder().setSerializedBinaryRows(ByteString.copyFrom(out.toByteArray())))
                .build());
    }

    static ReadRowsResponse arrow(int first, int rows) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, allocator)) {
            BigIntVector id = (BigIntVector) root.getVector("id");
            VarCharVector name = (VarCharVector) root.getVector("name");
            Float8Vector amount = (Float8Vector) root.getVector("amount");
            VarCharVector comment = (VarCharVector) root.getVector("comment");
            for (int row = 0; row < rows; row++) {
                int i = first + row;
                id.setSafe(row, i);
                name.setSafe(row, name(i).getBytes(StandardCharsets.UTF_8));
                amount.setSafe(row, i / 2.0);
                String text = comment(i);
                if (text == null) {
                    comment.setNull(row);
                } else {
                    comment.setSafe(row, text.getBytes(StandardCharsets.UTF_8));
                }
            }
            root.setRowCount(rows);
            try (org.apache.arrow.vector.ipc.message.ArrowRecordBatch batch =
                         new VectorUnloader(root).getRecordBatch()) {
                MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), batch);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return reparse(ReadRowsResponse.newBuilder()
                .setRowCount(rows)
                .setArrowRecordBatch(ArrowRecordBatch.newBuilder()
                        .setSerializedRecordBatch(ByteString.copyFrom(out.toByteArray())))
                .build());
    }

    private static String name(int i) {
        return "name-" + i;
    }

    private static String comment(int i) {
        return i % 2 == 0 ? null : "comment-" + i;
    }

    private static ReadRowsResponse reparse(ReadRowsResponse response) {
        try {
            return ReadRowsResponse.parseFrom(response.toByteString().newInput());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
