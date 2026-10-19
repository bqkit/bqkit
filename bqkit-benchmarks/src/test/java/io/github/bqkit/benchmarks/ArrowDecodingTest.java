package io.github.bqkit.benchmarks;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ArrowDecodingTest {

    @ParameterizedTest
    @EnumSource(ArrowDecoding.Variant.class)
    void decodesEveryRowOfConsecutiveResponses(ArrowDecoding.Variant variant) {
        List<Long> ids = new ArrayList<>();
        List<String> last = new ArrayList<>();
        try (ArrowDecoding decoding = new ArrowDecoding()) {
            for (int[] response : new int[][] {{0, 3_000}, {3_000, 2_000}}) {
                decoding.decode(variant, Payloads.arrow(response[0], response[1]), root -> {
                    BigIntVector id = (BigIntVector) root.getVector("id");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        ids.add(id.get(i));
                    }
                    int end = root.getRowCount() - 1;
                    last.clear();
                    last.add(new String(((VarCharVector) root.getVector("name")).get(end)));
                    last.add(String.valueOf(((Float8Vector) root.getVector("amount")).get(end)));
                    last.add(String.valueOf(root.getVector("comment").isNull(end - 1)));
                });
            }
        }

        assertThat(ids).containsExactlyElementsOf(LongStream.range(0, 5_000).boxed().toList());
        assertThat(last).containsExactly("name-4999", "2499.5", "true");
    }
}
