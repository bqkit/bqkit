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
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class QueryArrowReaderJobTest {

    private static final TableId DESTINATION = TableId.of("shop", "query_result");

    private final BigQuery bigQuery = mock(BigQuery.class);
    private final BigQueryReadClient readClient = mock(BigQueryReadClient.class);
    private final Job job = mock(Job.class);
    private final Job completed = mock(Job.class);
    private final QueryArrowReader reader = new QueryArrowReader(bigQuery, readClient);

    @BeforeEach
    void setUp() throws InterruptedException {
        when(bigQuery.getOptions()).thenReturn(BigQueryClients.bigQuery(new BigQueryConnection(
                "my-project", null, null, false, NoCredentials.getInstance())).getOptions());
        when(bigQuery.create(any(JobInfo.class))).thenReturn(job);
        when(job.waitFor()).thenReturn(completed);
        when(completed.getStatus()).thenReturn(mock(JobStatus.class));
        doReturn(QueryJobConfiguration.newBuilder("SELECT 1").setDestinationTable(DESTINATION).build())
                .when(completed).getConfiguration();
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(ArrowMessages.session());
    }

    @AfterEach
    void clearInterruptFlag() {
        Thread.interrupted();
    }

    @Test
    void truncates_the_explicit_destination_before_writing_the_result() {
        reader.query("SELECT 1", DESTINATION, batch -> { });

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

        assertThatThrownBy(() -> reader.query("SELEC 1", DESTINATION, batch -> { }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("BigQuery job failed: " + error);
        verify(readClient, never()).createReadSession(any(CreateReadSessionRequest.class));
    }

    @Test
    void reports_a_job_that_no_longer_exists() throws InterruptedException {
        when(job.waitFor()).thenReturn(null);

        assertThatThrownBy(() -> reader.query("SELECT 1", DESTINATION, batch -> { }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("BigQuery job not found");
    }

    @Test
    void keeps_the_thread_interrupted_when_waiting_for_the_job_is_interrupted() throws InterruptedException {
        InterruptedException interruption = new InterruptedException();
        when(job.waitFor()).thenThrow(interruption);

        assertThatThrownBy(() -> reader.query("SELECT 1", DESTINATION, batch -> { }))
                .isInstanceOf(IllegalStateException.class)
                .hasCause(interruption);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    @Test
    void reads_the_anonymous_table_the_job_wrote_to_when_no_destination_is_given() {
        TableId anonymous = TableId.of("my-project", "_anon", "anon_result");
        doReturn(QueryJobConfiguration.newBuilder("SELECT 1").setDestinationTable(anonymous).build())
                .when(completed).getConfiguration();

        reader.query("SELECT 1", batch -> { });

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
    void reports_a_job_that_wrote_no_destination_table() {
        doReturn(QueryJobConfiguration.newBuilder("SELECT 1").build()).when(completed).getConfiguration();

        assertThatThrownBy(() -> reader.query("SELECT 1", batch -> { }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("BigQuery job has no destination table");
    }
}
