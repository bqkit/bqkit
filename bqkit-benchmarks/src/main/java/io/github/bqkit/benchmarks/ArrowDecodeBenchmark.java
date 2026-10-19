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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/** Time and allocation to load one Arrow response into a reused root, per way of reading its payload. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 2, jvmArgsAppend = "--add-opens=java.base/java.nio=ALL-UNNAMED")
public class ArrowDecodeBenchmark {

    @Param({"1000", "10000", "50000"})
    private int rows;

    @Param
    private ArrowDecoding.Variant variant;

    private ReadRowsResponse response;
    private ArrowDecoding decoding;

    /** Builds the response once, outside the measurement. */
    @Setup
    public void setUp() {
        response = Payloads.arrow(0, rows);
        decoding = new ArrowDecoding();
    }

    /** Releases the Arrow buffers. */
    @TearDown
    public void tearDown() {
        decoding.close();
    }

    /** Loads the batch of the response and hands the root over. */
    @Benchmark
    public void decode(Blackhole blackhole) {
        decoding.decode(variant, response, blackhole::consume);
    }
}
