package io.github.bqkit.benchmarks;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/** Time and allocation to decode one Avro response, per way of handing its payload to the decoder. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(2)
public class AvroDecodeBenchmark {

    @Param({"1000", "10000", "50000"})
    private int rows;

    @Param
    private AvroDecoding.Variant variant;

    private ReadRowsResponse response;
    private AvroDecoding decoding;

    /** Builds the response once, outside the measurement. */
    @Setup
    public void setUp() {
        response = Payloads.avro(0, rows);
        decoding = new AvroDecoding();
    }

    /** Decodes every row of the response. */
    @Benchmark
    public void decode(Blackhole blackhole) {
        decoding.decode(variant, response, blackhole::consume);
    }
}
