package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.junit.jupiter.api.Test;

class BigQueryClientsTest {

    @Test
    void bigquery_client_targets_the_configured_project_and_rest_host() {
        BigQuery bigQuery = BigQueryClients.bigQuery(new BigQueryConnection(
                "my-project", "http://localhost:9050", null, false, NoCredentials.getInstance()));

        assertThat(bigQuery.getOptions().getProjectId()).isEqualTo("my-project");
        assertThat(bigQuery.getOptions().getHost()).isEqualTo("http://localhost:9050");
    }

    @Test
    void bigquery_client_keeps_the_google_endpoint_when_no_rest_host_is_set() {
        BigQuery bigQuery = BigQueryClients.bigQuery(new BigQueryConnection(
                "my-project", null, null, false, NoCredentials.getInstance()));

        String googleDefaultHost = BigQueryOptions.newBuilder()
                .setProjectId("my-project")
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getHost();
        assertThat(bigQuery.getOptions().getHost()).isEqualTo(googleDefaultHost);
    }
}
