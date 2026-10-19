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

- run a query into a destination table, then read that table in Avro through `BigQueryReadClient`;
- optional service account impersonation on top of the Application Default Credentials;
- a `@ServiceConnection` for the [BigQuery emulator](https://github.com/goccy/bigquery-emulator) with Testcontainers.

## Compatibility

Each major version of bqkit targets one Spring Boot generation:

- bqkit 1.0.x: Spring Boot 3.5, Java 21. This line only receives fixes.
- bqkit 2.x: Spring Boot 4 (planned).

## Modules

- `bqkit-core`: Spring-free clients (`BigQueryClients`, `CredentialsFactory`, `QueryAvroReader`).
- `bqkit-spring-boot-starter`: auto-configuration of `BigQuery`, `BigQueryReadClient` and `QueryAvroReader`.
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
```

```java
List<GenericRecord> rows = reader.query(
        "SELECT name FROM `my-project.shop.products` WHERE price > 1.5",
        TableId.of("shop", "query_result"));
```

`query(sql, destination)` truncates the destination table before writing. `query(sql)` reads the
anonymous result table that BigQuery creates for the job. `readTable(table)` reads an existing table.

Every bean backs off when the application defines its own. Defining a `BigQueryConnectionDetails`
bean replaces the connection built from the properties.

The starter ships the configuration metadata of the `bqkit.*` keys: your IDE completes and documents
them in `application.yml`.

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
```

```java
@Configuration
public class BigQueryConfig {

    @Bean
    BigQueryConnection bigQueryConnection(
            @Value("${bigquery.project-id}") String projectId,
            @Value("${bigquery.impersonate-service-account:}") String impersonateServiceAccount) {
        return new BigQueryConnection(projectId, null, null, false,
                CredentialsFactory.create(impersonateServiceAccount));
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

The `null` hosts select the Google endpoints. The `@ServiceConnection` of `bqkit-test` relies on the
starter.

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
and exposes the gRPC port of the Storage Read API.

### A complete test

The application code queries through `QueryAvroReader`:

```java
@Repository
public class ProductRepository {

    private static final TableId RESULT = TableId.of("shop", "products_in_stock");

    private final QueryAvroReader reader;

    public ProductRepository(QueryAvroReader reader) {
        this.reader = reader;
    }

    public List<String> findNamesInStock() {
        return reader.query("SELECT name FROM shop.products WHERE stock > 0", RESULT).stream()
                .map(row -> row.get("name").toString())
                .toList();
    }
}
```

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
- `query(sql)` fails on the emulator: it names the anonymous result table after the job id,
  which is not a valid Avro name ([goccy/bigquery-emulator#527](https://github.com/goccy/bigquery-emulator/issues/527)).
  Use `query(sql, destination)` in tests.
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
