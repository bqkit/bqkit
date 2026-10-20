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

    @Test
    void a_connection_has_no_location_unless_one_is_given() {
        BigQueryConnection connection = BigQueryConnection.google("my-project", NoCredentials.getInstance());

        assertThat(connection.location()).isNull();
    }

    @Test
    void with_location_returns_the_same_connection_in_that_location() {
        BigQueryConnection emulator =
                BigQueryConnection.emulator("testproject", "http://localhost:9050", "localhost:9060");

        BigQueryConnection located = emulator.withLocation("EU");

        assertThat(located.location()).isEqualTo("EU");
        assertThat(located.projectId()).isEqualTo("testproject");
        assertThat(located.restHost()).isEqualTo("http://localhost:9050");
        assertThat(located.grpcEndpoint()).isEqualTo("localhost:9060");
        assertThat(located.plaintext()).isTrue();
        assertThat(located.credentials()).isSameAs(NoCredentials.getInstance());
        assertThat(emulator.location()).isNull();
    }

    @Test
    void a_null_or_blank_location_means_no_location() {
        BigQueryConnection connection =
                BigQueryConnection.google("my-project", NoCredentials.getInstance()).withLocation("EU");

        assertThat(connection.withLocation(null).location()).isNull();
        assertThat(connection.withLocation("").location()).isNull();
        assertThat(connection.withLocation(" ").location()).isNull();
    }
}
