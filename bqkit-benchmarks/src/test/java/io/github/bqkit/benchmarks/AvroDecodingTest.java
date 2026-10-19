package io.github.bqkit.benchmarks;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.LongStream;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class AvroDecodingTest {

    @ParameterizedTest
    @EnumSource(AvroDecoding.Variant.class)
    void decodesEveryRowOfConsecutiveResponses(AvroDecoding.Variant variant) {
        AvroDecoding decoding = new AvroDecoding();
        List<GenericRecord> rows = new ArrayList<>();
        BiConsumer<ReadRowsResponse, List<GenericRecord>> decode =
                (response, into) -> decoding.decode(variant, response, into::add);

        decode.accept(Payloads.avro(0, 3_000), rows);
        decode.accept(Payloads.avro(3_000, 2_000), rows);

        assertThat(rows).extracting(row -> (Long) row.get("id"))
                .containsExactlyElementsOf(LongStream.range(0, 5_000).boxed().toList());
        assertThat(rows.get(4_999).get("name")).hasToString("name-4999");
        assertThat(rows.get(4_999).get("amount")).isEqualTo(2_499.5);
        assertThat(rows.get(4_999).get("comment")).hasToString("comment-4999");
        assertThat(rows.get(4_998).get("comment")).isNull();
    }
}
