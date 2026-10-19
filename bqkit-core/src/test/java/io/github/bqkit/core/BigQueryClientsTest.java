package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import java.io.IOException;
import java.util.Date;
import org.junit.jupiter.api.Test;

class BigQueryClientsTest {

    @Test
    void bigquery_client_targets_the_configured_project_and_rest_host() {
        BigQuery bigQuery = BigQueryClients.bigQuery(BigQueryConnection.emulator(
                "my-project", "http://localhost:9050", "localhost:9060"));

        assertThat(bigQuery.getOptions().getProjectId()).isEqualTo("my-project");
        assertThat(bigQuery.getOptions().getHost()).isEqualTo("http://localhost:9050");
    }

    @Test
    void bigquery_client_keeps_the_google_endpoint_when_no_rest_host_is_set() {
        BigQuery bigQuery = BigQueryClients.bigQuery(BigQueryConnection.google(
                "my-project", NoCredentials.getInstance()));

        String googleDefaultHost = BigQueryOptions.newBuilder()
                .setProjectId("my-project")
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getHost();
        assertThat(bigQuery.getOptions().getHost()).isEqualTo(googleDefaultHost);
    }

    @Test
    void read_client_sends_no_credentials_when_the_connection_has_none() {
        try (BigQueryReadClient client = BigQueryClients.readClient(BigQueryConnection.google(
                "my-project", NoCredentials.getInstance()))) {

            assertThat(client.getSettings().getCredentialsProvider()).isInstanceOf(NoCredentialsProvider.class);
        }
    }

    @Test
    void read_client_uses_the_connection_credentials() throws IOException {
        GoogleCredentials credentials = GoogleCredentials.create(new AccessToken("token", new Date(Long.MAX_VALUE)));
        try (BigQueryReadClient client = BigQueryClients.readClient(BigQueryConnection.google(
                "my-project", credentials))) {

            assertThat(client.getSettings().getCredentialsProvider().getCredentials()).isSameAs(credentials);
        }
    }

    @Test
    void read_client_targets_the_configured_grpc_endpoint() {
        try (BigQueryReadClient client = BigQueryClients.readClient(BigQueryConnection.emulator(
                "my-project", "http://localhost:9050", "localhost:9060"))) {

            assertThat(client.getSettings().getTransportChannelProvider())
                    .isInstanceOfSatisfying(InstantiatingGrpcChannelProvider.class,
                            channel -> assertThat(channel.getEndpoint()).isEqualTo("localhost:9060"));
        }
    }
}
