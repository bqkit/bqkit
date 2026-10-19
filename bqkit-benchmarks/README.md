# bqkit-benchmarks

JMH benchmarks, outside the default build and never published.

```shell
mvn -Pbenchmarks -pl bqkit-benchmarks -am package -DskipTests
java -jar bqkit-benchmarks/target/benchmarks.jar -prof gc
```

`AvroDecodeBenchmark` and `ArrowDecodeBenchmark` decode one `ReadRowsResponse` per operation. The `COPY` variant
is what `QueryAvroReader` and `QueryArrowReader` do; the other variants avoid the `ByteString.toByteArray()` copy.
Compare `gc.alloc.rate.norm` (bytes allocated per decoded response) as well as the time.
