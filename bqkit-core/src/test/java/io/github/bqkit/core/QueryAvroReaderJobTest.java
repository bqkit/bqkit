package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class QueryAvroReaderJobTest {

    private static final TableId DESTINATION = TableId.of("shop", "query_result");

    private static final QueryJobConfiguration CONFIGURED = QueryJobConfiguration.newBuilder(
                    "SELECT name FROM shop.products WHERE id = @id")
            .addNamedParameter("id", QueryParameterValue.int64(2))
            .setMaximumBytesBilled(1_000_000L)
            .setLabels(Map.of("team", "catalog"))
            .setDestinationTable(DESTINATION)
            .build();

    private final BigQuery bigQuery = mock(BigQuery.class);
    private final BigQueryReadClient readClient = mock(BigQueryReadClient.class);
    private final Job job = mock(Job.class);
    private final Job completed = mock(Job.class);
    private final QueryAvroReader reader = new QueryAvroReader(bigQuery, readClient);

    @BeforeEach
    void setUp() throws InterruptedException {
        when(bigQuery.getOptions()).thenReturn(BigQueryClients.bigQuery(
                BigQueryConnection.google("my-project", NoCredentials.getInstance())).getOptions());
        when(bigQuery.create(any(JobInfo.class))).thenReturn(job);
        when(job.waitFor()).thenReturn(completed);
        when(completed.getStatus()).thenReturn(mock(JobStatus.class));
        doReturn(QueryJobConfiguration.newBuilder("SELECT 1").setDestinationTable(DESTINATION).build())
                .when(completed).getConfiguration();
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(ReadSession.newBuilder()
                .setAvroSchema(AvroSchema.newBuilder().setSchema("""
                        {"type": "record", "name": "row", "fields": []}"""))
                .build());
    }

    @AfterEach
    void clearInterruptFlag() {
        Thread.interrupted();
    }

    @Test
    void truncates_the_explicit_destination_before_writing_the_result() {
        reader.query("SELECT 1", DESTINATION);

        ArgumentCaptor<JobInfo> submitted = ArgumentCaptor.forClass(JobInfo.class);
        verify(bigQuery).create(submitted.capture());
        QueryJobConfiguration configuration = submitted.getValue().getConfiguration();
        assertThat(configuration.getDestinationTable()).isEqualTo(DESTINATION);
        assertThat(configuration.getWriteDisposition()).isEqualTo(JobInfo.WriteDisposition.WRITE_TRUNCATE);
    }

    @Test
    void reports_a_failed_job_instead_of_reading_its_destination() {
        BigQueryError error = new BigQueryError("invalidQuery", "query", "Syntax error");
        JobStatus failed = mock(JobStatus.class);
        when(failed.getError()).thenReturn(error);
        when(completed.getStatus()).thenReturn(failed);

        assertThatThrownBy(() -> reader.query("SELEC 1", DESTINATION))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("BigQuery job failed: " + error);
        verify(readClient, never()).createReadSession(any(CreateReadSessionRequest.class));
    }

    @Test
    void reports_a_job_that_no_longer_exists() throws InterruptedException {
        when(job.waitFor()).thenReturn(null);

        assertThatThrownBy(() -> reader.query("SELECT 1", DESTINATION))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("BigQuery job not found");
    }

    @Test
    void keeps_the_thread_interrupted_when_waiting_for_the_job_is_interrupted() throws InterruptedException {
        InterruptedException interruption = new InterruptedException();
        when(job.waitFor()).thenThrow(interruption);

        assertThatThrownBy(() -> reader.query("SELECT 1", DESTINATION))
                .isInstanceOf(IllegalStateException.class)
                .hasCause(interruption);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    @Test
    void reads_the_anonymous_table_the_job_wrote_to_when_no_destination_is_given() {
        TableId anonymous = TableId.of("my-project", "_anon", "anon_result");
        doReturn(QueryJobConfiguration.newBuilder("SELECT 1").setDestinationTable(anonymous).build())
                .when(completed).getConfiguration();

        reader.query("SELECT 1");

        ArgumentCaptor<JobInfo> submitted = ArgumentCaptor.forClass(JobInfo.class);
        verify(bigQuery).create(submitted.capture());
        QueryJobConfiguration configuration = submitted.getValue().getConfiguration();
        assertThat(configuration.getQuery()).isEqualTo("SELECT 1");
        assertThat(configuration.getDestinationTable()).isNull();
        ArgumentCaptor<CreateReadSessionRequest> request = ArgumentCaptor.forClass(CreateReadSessionRequest.class);
        verify(readClient).createReadSession(request.capture());
        assertThat(request.getValue().getReadSession().getTable())
                .isEqualTo("projects/my-project/datasets/_anon/tables/anon_result");
    }

    @Test
    void streams_the_anonymous_table_the_job_wrote_to_when_no_destination_is_given() {
        TableId anonymous = TableId.of("my-project", "_anon", "anon_result");
        doReturn(QueryJobConfiguration.newBuilder("SELECT 1").setDestinationTable(anonymous).build())
                .when(completed).getConfiguration();

        reader.query("SELECT 1", row -> { });

        ArgumentCaptor<JobInfo> submitted = ArgumentCaptor.forClass(JobInfo.class);
        verify(bigQuery).create(submitted.capture());
        QueryJobConfiguration configuration = submitted.getValue().getConfiguration();
        assertThat(configuration.getQuery()).isEqualTo("SELECT 1");
        assertThat(configuration.getDestinationTable()).isNull();
        ArgumentCaptor<CreateReadSessionRequest> request = ArgumentCaptor.forClass(CreateReadSessionRequest.class);
        verify(readClient).createReadSession(request.capture());
        assertThat(request.getValue().getReadSession().getTable())
                .isEqualTo("projects/my-project/datasets/_anon/tables/anon_result");
    }

    @Test
    void runs_the_given_job_configuration_as_is() {
        reader.query(CONFIGURED);

        ArgumentCaptor<JobInfo> submitted = ArgumentCaptor.forClass(JobInfo.class);
        verify(bigQuery).create(submitted.capture());
        assertThat((QueryJobConfiguration) submitted.getValue().getConfiguration()).isEqualTo(CONFIGURED);
    }

    @Test
    void streams_the_result_of_the_given_job_configuration_as_is() {
        reader.query(CONFIGURED, row -> { });

        ArgumentCaptor<JobInfo> submitted = ArgumentCaptor.forClass(JobInfo.class);
        verify(bigQuery).create(submitted.capture());
        assertThat((QueryJobConfiguration) submitted.getValue().getConfiguration()).isEqualTo(CONFIGURED);
        ArgumentCaptor<CreateReadSessionRequest> request = ArgumentCaptor.forClass(CreateReadSessionRequest.class);
        verify(readClient).createReadSession(request.capture());
        assertThat(request.getValue().getReadSession().getTable())
                .isEqualTo("projects/my-project/datasets/shop/tables/query_result");
    }

    @Test
    void rejects_a_null_consumer_before_running_the_query() {
        assertThatThrownBy(() -> reader.query("SELECT 1", (Consumer<GenericRecord>) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> reader.query("SELECT 1", TableId.of("shop", "result"), null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> reader.query(CONFIGURED, null))
                .isInstanceOf(NullPointerException.class);

        verify(bigQuery, never()).create(any(JobInfo.class));
    }

    @Test
    void rejects_a_null_configuration_before_running_the_query() {
        assertThatThrownBy(() -> reader.query((QueryJobConfiguration) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("configuration");
        assertThatThrownBy(() -> reader.query((QueryJobConfiguration) null, rows -> { }))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("configuration");

        verify(bigQuery, never()).create(any(JobInfo.class));
    }

    @Test
    void rejects_a_null_consumer_before_opening_a_read_session() {
        assertThatThrownBy(() -> reader.readTable(TableId.of("shop", "products"), null))
                .isInstanceOf(NullPointerException.class);

        verify(readClient, never()).createReadSession(any(CreateReadSessionRequest.class));
    }

    @Test
    void reports_a_job_that_wrote_no_destination_table() {
        doReturn(QueryJobConfiguration.newBuilder("SELECT 1").build()).when(completed).getConfiguration();

        assertThatThrownBy(() -> reader.query("SELECT 1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("BigQuery job has no destination table");
    }
}
