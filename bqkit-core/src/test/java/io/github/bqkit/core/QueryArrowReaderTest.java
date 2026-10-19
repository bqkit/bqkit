package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BigQueryEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class QueryArrowReaderTest {

    @Container
    static final BigQueryEmulatorContainer EMULATOR = Emulator.container();

    static BigQuery bigQuery;
    static BigQueryReadClient readClient;
    static QueryArrowReader reader;

    @BeforeAll
    static void setUp() {
        BigQueryConnection connection = Emulator.connection(EMULATOR);
        bigQuery = BigQueryClients.bigQuery(connection);
        readClient = BigQueryClients.readClient(connection);
        reader = new QueryArrowReader(bigQuery, readClient);
        Emulator.seed(bigQuery);
    }

    @AfterAll
    static void tearDown() {
        readClient.close();
    }

    @Test
    void reads_a_table_in_arrow_through_the_storage_read_api() {
        List<String> names = new ArrayList<>();

        reader.readTable(Emulator.PRODUCTS, batch -> names.addAll(names(batch)));

        assertThat(names).containsExactlyInAnyOrder("apple", "banana", "cherry");
    }

    @Test
    void runs_a_query_into_an_explicit_destination_then_reads_it_in_arrow() {
        List<String> names = new ArrayList<>();

        reader.query("SELECT id, name FROM `" + Emulator.PROJECT_ID + ".shop.products` WHERE price > 1.5",
                TableId.of("shop", "query_result"), batch -> names.addAll(names(batch)));

        assertThat(names).containsExactlyInAnyOrder("banana", "cherry");
    }

    @Test
    void runs_a_query_then_reads_its_anonymous_result_table_in_arrow() {
        List<String> names = new ArrayList<>();

        reader.query("SELECT id, name FROM `" + Emulator.PROJECT_ID + ".shop.products` WHERE price > 1.5",
                batch -> names.addAll(names(batch)));

        assertThat(names).containsExactlyInAnyOrder("banana", "cherry");
    }

    private static List<String> names(VectorSchemaRoot batch) {
        VarCharVector name = (VarCharVector) batch.getVector("name");
        List<String> names = new ArrayList<>();
        for (int i = 0; i < batch.getRowCount(); i++) {
            names.add(name.getObject(i).toString());
        }
        return names;
    }
}
