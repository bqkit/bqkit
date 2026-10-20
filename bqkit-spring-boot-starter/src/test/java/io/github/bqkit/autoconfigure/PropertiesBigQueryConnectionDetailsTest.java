package io.github.bqkit.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import io.github.bqkit.core.BigQueryConnection;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PropertiesBigQueryConnectionDetailsTest {

    @Test
    void connection_runs_the_jobs_in_the_configured_location() {
        PropertiesBigQueryConnectionDetails details = new PropertiesBigQueryConnectionDetails(
                new BqkitProperties("my-project", null, "EU", 1), NoCredentials::getInstance);

        assertThat(details.connection().location()).isEqualTo("EU");
    }

    @Test
    void an_empty_location_property_lets_bigquery_pick_the_location() {
        PropertiesBigQueryConnectionDetails details = new PropertiesBigQueryConnectionDetails(
                new BqkitProperties("my-project", null, "", 1), NoCredentials::getInstance);

        assertThat(details.connection().location()).isNull();
    }

    @Test
    void shares_the_credentials_between_the_clients() {
        AtomicInteger created = new AtomicInteger();
        PropertiesBigQueryConnectionDetails details = new PropertiesBigQueryConnectionDetails(
                new BqkitProperties("my-project", null, null, 1), () -> {
                    created.incrementAndGet();
                    return NoCredentials.getInstance();
                });

        Credentials rest = details.connection().credentials();
        Credentials grpc = details.connection().credentials();

        assertThat(grpc).isSameAs(rest);
        assertThat(created).hasValue(1);
    }

    @Test
    void resolves_the_credentials_only_when_a_client_reads_them() {
        AtomicInteger created = new AtomicInteger();

        PropertiesBigQueryConnectionDetails details = new PropertiesBigQueryConnectionDetails(
                new BqkitProperties("my-project", null, null, 1), () -> {
                    created.incrementAndGet();
                    return NoCredentials.getInstance();
                });

        BigQueryConnection connection = details.connection();

        assertThat(created).hasValue(0);

        connection.credentials();

        assertThat(created).hasValue(1);
    }

    @Test
    void resolves_the_credentials_again_after_a_failure() {
        AtomicInteger attempts = new AtomicInteger();
        PropertiesBigQueryConnectionDetails details = new PropertiesBigQueryConnectionDetails(
                new BqkitProperties("my-project", null, null, 1), () -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new UncheckedIOException(new IOException("no Application Default Credentials"));
                    }
                    return NoCredentials.getInstance();
                });

        assertThatThrownBy(() -> details.connection().credentials()).isInstanceOf(UncheckedIOException.class);

        assertThat(details.connection().credentials()).isSameAs(NoCredentials.getInstance());
        assertThat(attempts).hasValue(2);
    }
}
