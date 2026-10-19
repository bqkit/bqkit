package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import java.util.ArrayList;
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

    static BigQuery bigQuery;
    static BigQueryReadClient readClient;
    static QueryAvroReader reader;

    @BeforeAll
    static void setUp() {
        BigQueryConnection connection = Emulator.connection(EMULATOR);
        bigQuery = BigQueryClients.bigQuery(connection);
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
    void streams_a_table_row_by_row() {
        List<String> names = new ArrayList<>();

        reader.readTable(Emulator.PRODUCTS, row -> names.add(row.get("name").toString()));

        assertThat(names).containsExactlyInAnyOrder("apple", "banana", "cherry");
    }

    @Test
    void runs_a_query_into_an_explicit_destination_then_streams_it_row_by_row() {
        List<String> names = new ArrayList<>();

        reader.query("SELECT id, name FROM `" + Emulator.PROJECT_ID + ".shop.products` WHERE price > 1.5",
                TableId.of("shop", "streamed_result"), row -> names.add(row.get("name").toString()));

        assertThat(names).containsExactlyInAnyOrder("banana", "cherry");
    }

    @Test
    void runs_a_parameterized_job_configuration_then_reads_its_destination_in_avro() {
        List<GenericRecord> rows = reader.query(QueryJobConfiguration.newBuilder(
                        "SELECT name FROM `" + Emulator.PROJECT_ID + ".shop.products` WHERE id = @id")
                .addNamedParameter("id", QueryParameterValue.int64(2))
                .setDestinationTable(TableId.of("shop", "parameterized_result"))
                .build());

        assertThat(rows).extracting(r -> r.get("name").toString()).containsExactly("banana");
    }

    @Test
    @Disabled("bigquery-emulator 0.8.1 names the anonymous result table after the job id, an invalid Avro name: goccy/bigquery-emulator#527")
    void runs_a_query_then_reads_its_anonymous_result_table_in_avro() {
        List<GenericRecord> rows = reader.query(
                "SELECT id, name FROM `" + Emulator.PROJECT_ID + ".shop.products` WHERE price > 1.5");

        assertThat(rows).extracting(r -> r.get("name").toString())
                .containsExactlyInAnyOrder("banana", "cherry");
    }

    @Test
    void runs_a_query_without_destination_then_reads_the_rows_the_job_wrote() throws InterruptedException {
        BigQuery jobRunner = mock(BigQuery.class);
        Job job = mock(Job.class);
        Job completed = mock(Job.class);
        when(jobRunner.getOptions()).thenReturn(bigQuery.getOptions());
        when(jobRunner.create(any(JobInfo.class))).thenReturn(job);
        when(job.waitFor()).thenReturn(completed);
        when(completed.getStatus()).thenReturn(mock(JobStatus.class));
        doReturn(QueryJobConfiguration.newBuilder("SELECT 1").setDestinationTable(Emulator.PRODUCTS).build())
                .when(completed).getConfiguration();

        List<GenericRecord> rows = new QueryAvroReader(jobRunner, readClient).query("SELECT 1");

        assertThat(rows).extracting(r -> r.get("name").toString())
                .containsExactlyInAnyOrder("apple", "banana", "cherry");
    }
}
