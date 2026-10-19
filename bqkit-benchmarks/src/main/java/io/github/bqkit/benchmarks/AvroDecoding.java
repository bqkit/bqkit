package io.github.bqkit.benchmarks;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Consumer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

/** Ways of decoding the Avro rows of a response; {@link Variant#COPY} is what {@code QueryAvroReader} does. */
public final class AvroDecoding {

    /** How the payload reaches the decoder. */
    public enum Variant {
        /** Copies the payload into a byte array and creates a decoder for every response. */
        COPY,
        /** Copies the payload into a byte array and reuses one decoder across responses. */
        COPY_REUSING_DECODER,
        /** Reads the payload through {@link ByteString#newInput()} and reuses one decoder across responses. */
        STREAM_REUSING_DECODER
    }

    private final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(Payloads.AVRO_SCHEMA);
    private BinaryDecoder reused;

    void decode(Variant variant, ReadRowsResponse response, Consumer<GenericRecord> rows) {
        ByteString payload = response.getAvroRows().getSerializedBinaryRows();
        BinaryDecoder decoder = switch (variant) {
            case COPY -> DecoderFactory.get().binaryDecoder(payload.toByteArray(), null);
            case COPY_REUSING_DECODER -> DecoderFactory.get().binaryDecoder(payload.toByteArray(), reused);
            case STREAM_REUSING_DECODER -> DecoderFactory.get().binaryDecoder(payload.newInput(), reused);
        };
        if (variant != Variant.COPY) {
            reused = decoder;
        }
        try {
            while (!decoder.isEnd()) {
                rows.accept(datumReader.read(null, decoder));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
