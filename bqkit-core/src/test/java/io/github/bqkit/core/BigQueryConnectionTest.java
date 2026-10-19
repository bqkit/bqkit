package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import java.util.Date;
import org.junit.jupiter.api.Test;

class BigQueryConnectionTest {

    @Test
    void google_connection_uses_the_google_endpoints_over_tls_with_the_given_credentials() {
        GoogleCredentials credentials = GoogleCredentials.create(new AccessToken("token", new Date(Long.MAX_VALUE)));

        BigQueryConnection connection = BigQueryConnection.google("my-project", credentials);

        assertThat(connection.projectId()).isEqualTo("my-project");
        assertThat(connection.restHost()).isNull();
        assertThat(connection.grpcEndpoint()).isNull();
        assertThat(connection.plaintext()).isFalse();
        assertThat(connection.credentials()).isSameAs(credentials);
    }

    @Test
    void emulator_connection_targets_the_given_endpoints_in_plaintext_without_credentials() {
        BigQueryConnection connection =
                BigQueryConnection.emulator("testproject", "http://localhost:9050", "localhost:9060");

        assertThat(connection.projectId()).isEqualTo("testproject");
        assertThat(connection.restHost()).isEqualTo("http://localhost:9050");
        assertThat(connection.grpcEndpoint()).isEqualTo("localhost:9060");
        assertThat(connection.plaintext()).isTrue();
        assertThat(connection.credentials()).isSameAs(NoCredentials.getInstance());
    }
}
