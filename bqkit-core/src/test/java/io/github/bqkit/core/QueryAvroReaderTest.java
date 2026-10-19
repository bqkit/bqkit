package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BigQueryEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class QueryAvroReaderTest {

    @Container
    static final BigQueryEmulatorContainer EMULATOR = Emulator.container();

    static BigQueryReadClient readClient;
    static QueryAvroReader reader;

    @BeforeAll
    static void setUp() {
        BigQueryConnection connection = Emulator.connection(EMULATOR);
        BigQuery bigQuery = BigQueryClients.bigQuery(connection);
        readClient = BigQueryClients.readClient(connection);
        reader = new QueryAvroReader(bigQuery, readClient);
        Emulator.seed(bigQuery);
    }

    @AfterAll
    static void tearDown() {
        readClient.close();
    }

    @Test
    void reads_a_table_in_avro_through_the_storage_read_api() {
        List<GenericRecord> rows = reader.readTable(Emulator.PRODUCTS);

        assertThat(rows).extracting(r -> r.get("name").toString())
                .containsExactlyInAnyOrder("apple", "banana", "cherry");
    }

    @Test
    void runs_a_query_into_an_explicit_destination_then_reads_it_in_avro() {
        List<GenericRecord> rows = reader.query(
                "SELECT id, name FROM `" + Emulator.PROJECT_ID + ".shop.products` WHERE price > 1.5",
                TableId.of("shop", "query_result"));

        assertThat(rows).extracting(r -> r.get("name").toString())
                .containsExactlyInAnyOrder("banana", "cherry");
    }

    @Test
    @Disabled("bigquery-emulator 0.8.1 names the anonymous result table after the job id, an invalid Avro name: goccy/bigquery-emulator#527")
    void runs_a_query_then_reads_its_anonymous_result_table_in_avro() {
        List<GenericRecord> rows = reader.query(
                "SELECT id, name FROM `" + Emulator.PROJECT_ID + ".shop.products` WHERE price > 1.5");

        assertThat(rows).extracting(r -> r.get("name").toString())
                .containsExactlyInAnyOrder("banana", "cherry");
    }
}
