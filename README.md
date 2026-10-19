# bqkit

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

```xml
<dependency>
    <groupId>io.github.bqkit</groupId>
    <artifactId>bqkit-spring-boot-starter</artifactId>
    <version>1.0.0-SNAPSHOT</version>
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

## Testing

```xml
<dependency>
    <groupId>io.github.bqkit</groupId>
    <artifactId>bqkit-test</artifactId>
    <version>1.0.0-SNAPSHOT</version>
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
