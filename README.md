<p align="center">
  <img src="docs/assets/logo-bq.png" alt="bqkit" width="128">
</p>

# bqkit

[![CI Build](https://github.com/bqkit/bqkit/actions/workflows/ci.yml/badge.svg?branch=release/1.x)](https://github.com/bqkit/bqkit/actions/workflows/ci.yml?query=branch%3Arelease%2F1.x)
[![Qodana](https://github.com/bqkit/bqkit/actions/workflows/qodana_code_quality.yml/badge.svg?branch=release/1.x)](https://github.com/bqkit/bqkit/actions/workflows/qodana_code_quality.yml?query=branch%3Arelease%2F1.x)
[![codecov](https://codecov.io/gh/bqkit/bqkit/branch/release%2F1.x/graph/badge.svg)](https://codecov.io/gh/bqkit/bqkit/tree/release%2F1.x)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java](https://img.shields.io/badge/Java-21+-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5-green.svg)](https://spring.io/projects/spring-boot)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.bqkit/bqkit-spring-boot-starter.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.bqkit/bqkit-spring-boot-starter)
[![Website](https://img.shields.io/badge/Website-bqkit.github.io-0ea5e9.svg)](https://bqkit.github.io/bqkit/)

Spring Boot integration for Google BigQuery, built around the Storage Read API:

- run a query into a destination table, then read that table through `BigQueryReadClient`, in Avro
  (`QueryAvroReader`) or in Arrow (`QueryArrowReader`);
- optional service account impersonation on top of the Application Default Credentials;
- a `@ServiceConnection` for the [BigQuery emulator](https://github.com/goccy/bigquery-emulator) with Testcontainers.

## Compatibility

Each major version of bqkit targets one Spring Boot generation:

- bqkit 1.0.x: Spring Boot 3.5, Java 21. This line only receives fixes.
- bqkit 2.x: Spring Boot 4 (planned).

## Modules

- `bqkit-core`: Spring-free clients (`BigQueryClients`, `CredentialsFactory`, `QueryAvroReader`,
  `QueryArrowReader`).
- `bqkit-spring-boot-starter`: auto-configuration of `BigQuery`, `BigQueryReadClient`, `QueryAvroReader`
  and `QueryArrowReader`.
- `bqkit-test`: emulator container and `@ServiceConnection` support for tests.

## Usage

`${bqkit.version}` is the latest version shown by the badge above. Until the first release,
build bqkit locally with `mvn install` and use the version of the pom.

```xml
<dependency>
    <groupId>io.github.bqkit</groupId>
    <artifactId>bqkit-spring-boot-starter</artifactId>
    <version>${bqkit.version}</version>
</dependency>
```

```yaml
bqkit:
  bigquery:
    project-id: my-project
    impersonate-service-account: reader@my-project.iam.gserviceaccount.com # optional
    location: EU # optional, location of the query jobs
    max-read-streams: 4 # optional, 1 by default
```

```java
List<GenericRecord> rows = reader.query(
        "SELECT name FROM `my-project.shop.products` WHERE price > 1.5",
        TableId.of("shop", "query_result"));
```

`query(sql, destination)` truncates the destination table before writing. `query(sql)` reads the
anonymous result table that BigQuery creates for the job. `readTable(table)` reads an existing table.

Concurrent calls into the same destination table race: each one truncates the table, so a call can
read the rows of another, or none. Give each call its own destination table, or use `query(sql)`, whose
anonymous result table belongs to its job.

`query(configuration)` runs a `QueryJobConfiguration` as is, for query parameters, `maximumBytesBilled`,
labels or location. It sets no write disposition: set the destination and `WRITE_TRUNCATE` yourself if you
need them.

```java
List<GenericRecord> rows = reader.query(QueryJobConfiguration.newBuilder(
                "SELECT name FROM `my-project.shop.products` WHERE price > @price")
        .addNamedParameter("price", QueryParameterValue.float64(1.5))
        .setMaximumBytesBilled(1_000_000_000L)
        .build());
```

The read session is created in the connection project (`project-id`), which needs
`bigquery.readsessions.create` and pays for the read, whatever the project of the table. The service
account of the connection also needs `bigquery.readsessions.getData` on the table. `project-id` is the
connection project when it is set; otherwise the Google client library picks the project of the credentials
or `GOOGLE_CLOUD_PROJECT`, and only if it finds none is the project of the table used. Set `project-id` to
be sure of who pays.

### Avro or Arrow

The starter declares both readers: inject the one that fits the use case.

- `QueryAvroReader` reads row by row. It returns the rows as a `List<GenericRecord>`, which holds the
  whole result in memory, or hands each row to a callback, for large results.
- `QueryArrowReader` hands the result to a callback, one columnar record batch at a time: to work on
  columns.

`QueryAvroReader` streams the rows with `query(sql, rows)`, `query(sql, destination, rows)`,
`query(configuration, rows)` or `readTable(table, rows)`:

```java
reader.query("SELECT name FROM `my-project.shop.products`", TableId.of("shop", "query_result"),
        row -> System.out.println(row.get("name")));
```

The next response of the stream is only requested once every row of the current one has gone through
the callback, so a slow callback slows the read down instead of filling the memory. If a row cannot be decoded or the callback throws, the read stops and
the stream is cancelled: the rows before it have already been handed to the callback.

`QueryAvroReader` returns BigQuery values as Java types:

- `STRING` as `String`;
- `TIMESTAMP` as `Instant`;
- `DATE` as `LocalDate`;
- `TIME` as `LocalTime`;
- `NUMERIC` and `BIGNUMERIC` as `BigDecimal`;
- `INT64` as `Long`, `FLOAT64` as `Double`, `BOOL` as `Boolean`, `BYTES` as `ByteBuffer`;
- `DATETIME`, `GEOGRAPHY` and `JSON` as `String`, as BigQuery sends them;
- `STRUCT` as a nested `GenericRecord`, `ARRAY` as a `List`.

A `NULL` value is `null`.

`QueryArrowReader` takes the same arguments, and calls the callback once per record batch:

```java
reader.query("SELECT name, price FROM `my-project.shop.products`", TableId.of("shop", "query_result"), batch -> {
    VarCharVector name = (VarCharVector) batch.getVector("name");
    for (int i = 0; i < batch.getRowCount(); i++) {
        System.out.println(name.getObject(i));
    }
});
```

The batch is released once the callback returns: copy what you keep. Arrow needs access to `java.nio`
on Java 21: start the application, and the tests, with
`--add-opens=java.base/java.nio=ALL-UNNAMED`. Without it, the first read fails with
`Failed to initialize MemoryUtil`.

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <configuration>
        <argLine>--add-opens=java.base/java.nio=ALL-UNNAMED</argLine>
    </configuration>
</plugin>
```

### Parallel reads

A read session reads one stream by default. `max-read-streams`, or the `maxStreams` argument of the
reader constructors without the starter, asks BigQuery for up to that many streams and reads them at the
same time, each on its own virtual thread. BigQuery may open fewer streams than asked, for a small table.

```java
QueryAvroReader reader = new QueryAvroReader(bigQuery, readClient, 4);
```

With more than one stream:

- the rows come in no order, even with an `ORDER BY`: keep the default of 1 when the order matters;
- the callback is called by one stream at a time, so it does not have to be thread-safe, and a stream
  waiting for the callback requests no new response;
- `QueryArrowReader` hands each stream its own batch: copy what you keep, as with a single stream;
- a stream waiting for the callback holds the batch it decoded: with N streams, up to N decoded batches
  stay in memory at once, off-heap for `QueryArrowReader`, so size the memory before raising the value;
- the first failure, a row that cannot be decoded or a callback that throws, stops the read: the other
  streams are cancelled and that failure is thrown.

Every bean backs off when the application defines its own. Defining a `BigQueryConnectionDetails`
bean replaces the connection built from the properties.

The starter ships the configuration metadata of the `bqkit.*` keys: your IDE completes and documents
them in `application.yml`.

### With spring-cloud-gcp

With `spring-cloud-gcp-starter-bigquery` on the classpath, bqkit runs after it and reuses its `BigQuery`
client: the queries run with the `spring.cloud.gcp.*` project and credentials. The Storage Read client
still comes from `bqkit.bigquery.*`. Set the same project and credentials on both sides, or declare your
own `BigQueryReadClient` bean, so the query and the read use the same identity.

### Without the starter

`bqkit-core` has no Spring dependency. Declare the beans yourself, with keys of your own in
`application.yml`:

```xml
<dependency>
    <groupId>io.github.bqkit</groupId>
    <artifactId>bqkit-core</artifactId>
    <version>${bqkit.version}</version>
</dependency>
```

```yaml
bigquery:
  project-id: my-project
  impersonate-service-account: reader@my-project.iam.gserviceaccount.com # optional
  location: EU # optional, location of the query jobs
```

```java
@Configuration
public class BigQueryConfig {

    @Bean
    BigQueryConnection bigQueryConnection(
            @Value("${bigquery.project-id}") String projectId,
            @Value("${bigquery.impersonate-service-account:}") String impersonateServiceAccount,
            @Value("${bigquery.location:}") String location) {
        return BigQueryConnection.google(projectId, CredentialsFactory.create(impersonateServiceAccount))
                .withLocation(location);
    }

    @Bean
    BigQuery bigQuery(BigQueryConnection connection) {
        return BigQueryClients.bigQuery(connection);
    }

    @Bean(destroyMethod = "close")
    BigQueryReadClient bigQueryReadClient(BigQueryConnection connection) {
        return BigQueryClients.readClient(connection);
    }

    @Bean
    QueryAvroReader queryAvroReader(BigQuery bigQuery, BigQueryReadClient readClient) {
        return new QueryAvroReader(bigQuery, readClient);
    }
}
```

`BigQueryConnection.google(...)` targets the Google endpoints; `BigQueryConnection.emulator(...)` targets an
emulator. The `@ServiceConnection` of `bqkit-test` relies on the starter.

## Testing

```xml
<dependency>
    <groupId>io.github.bqkit</groupId>
    <artifactId>bqkit-test</artifactId>
    <version>${bqkit.version}</version>
    <scope>test</scope>
</dependency>
```

```java
@SpringBootTest
@Testcontainers
class ProductRepositoryTest {

    @Container
    @ServiceConnection
    static final BqkitBigQueryEmulatorContainer EMULATOR = new BqkitBigQueryEmulatorContainer();
}
```

`BqkitBigQueryEmulatorContainer` runs `ghcr.io/goccy/bigquery-emulator:0.8.1` on project `testproject`
and exposes the gRPC port of the Storage Read API. `@ServiceConnection` only accepts this container: the
Testcontainers `BigQueryEmulatorContainer` does not expose the gRPC port.

### A complete test

The application code queries through `QueryAvroReader`:

```java
@Repository
public class ProductRepository {

    private final QueryAvroReader reader;

    public ProductRepository(QueryAvroReader reader) {
        this.reader = reader;
    }

    public List<String> findNamesInStock() {
        TableId result = TableId.of("shop", "products_in_stock_" + UUID.randomUUID().toString().replace('-', '_'));
        return reader.query("SELECT name FROM shop.products WHERE stock > 0", result).stream()
                .map(row -> (String) row.get("name"))
                .toList();
    }
}
```

Each call writes to its own destination table, so concurrent calls do not race. Set a default table
expiration on the dataset so that these tables do not pile up.

The test seeds the emulator with the `BigQuery` bean, calls the repository, then checks the result:

```java
@SpringBootTest
@Testcontainers
class ProductRepositoryTest {

    @Container
    @ServiceConnection
    static final BqkitBigQueryEmulatorContainer EMULATOR = new BqkitBigQueryEmulatorContainer();

    private static final TableId PRODUCTS = TableId.of("shop", "products");

    @Autowired
    BigQuery bigQuery;

    @Autowired
    ProductRepository repository;

    @Test
    void finds_the_names_of_the_products_in_stock() {
        // Given
        bigQuery.create(DatasetInfo.of(PRODUCTS.getDataset()));
        bigQuery.create(TableInfo.of(PRODUCTS, StandardTableDefinition.of(Schema.of(
                Field.of("name", StandardSQLTypeName.STRING),
                Field.of("stock", StandardSQLTypeName.INT64)))));
        bigQuery.insertAll(InsertAllRequest.newBuilder(PRODUCTS)
                .addRow(Map.of("name", "banana", "stock", 3))
                .addRow(Map.of("name", "apple", "stock", 0))
                .addRow(Map.of("name", "cherry", "stock", 12))
                .build());

        // When
        List<String> names = repository.findNamesInStock();

        // Then
        assertThat(names).containsExactlyInAnyOrder("banana", "cherry");
    }
}
```

The emulator keeps its data for the lifetime of the container: with several tests in a class, give
each its own dataset or destination table (see the limits below).

### Emulator limits

- The emulator builds the Avro namespace from the project id, so a project id with a hyphen
  (such as `test-project`, the Testcontainers default) breaks Avro reads.
  [testcontainers/testcontainers-java#12123](https://github.com/testcontainers/testcontainers-java/pull/12123)
  proposes `withProjectId`.
- `QueryAvroReader.query(sql)` fails on the emulator: it names the anonymous result table after the
  job id, which is not a valid Avro name ([goccy/bigquery-emulator#527](https://github.com/goccy/bigquery-emulator/issues/527)).
  Use `query(sql, destination)` in tests. `QueryArrowReader` is not affected.
- The emulator fails to send a `TIME` column in Avro (`cannot transform to binary time-micros`).
- The emulator ignores `WRITE_TRUNCATE`: `query(sql, destination)` appends to the destination
  table instead of replacing it, so a second query into the same table returns the rows of both.
  Use a distinct destination table per test, or drop it before each test.
- See also [goccy/bigquery-emulator#508](https://github.com/goccy/bigquery-emulator/pull/508).

## Build

```sh
mvn verify
```

The tests need Docker.

## License

[Apache License 2.0](LICENSE)
