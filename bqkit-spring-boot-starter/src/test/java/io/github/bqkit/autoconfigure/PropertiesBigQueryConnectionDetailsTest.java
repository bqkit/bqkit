package io.github.bqkit.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PropertiesBigQueryConnectionDetailsTest {

    @Test
    void connection_runs_the_jobs_in_the_configured_location() {
        PropertiesBigQueryConnectionDetails details = new PropertiesBigQueryConnectionDetails(
                new BqkitProperties("my-project", null, "EU"), NoCredentials::getInstance);

        assertThat(details.connection().location()).isEqualTo("EU");
    }

    @Test
    void shares_the_credentials_between_the_clients() {
        AtomicInteger created = new AtomicInteger();
        PropertiesBigQueryConnectionDetails details = new PropertiesBigQueryConnectionDetails(
                new BqkitProperties("my-project", null, null), () -> {
                    created.incrementAndGet();
                    return NoCredentials.getInstance();
                });

        Credentials rest = details.connection().credentials();
        Credentials grpc = details.connection().credentials();

        assertThat(grpc).isSameAs(rest);
        assertThat(created).hasValue(1);
    }

    @Test
    void resolves_the_credentials_only_when_a_client_is_built() {
        AtomicInteger created = new AtomicInteger();

        PropertiesBigQueryConnectionDetails details = new PropertiesBigQueryConnectionDetails(
                new BqkitProperties("my-project", null, null), () -> {
                    created.incrementAndGet();
                    return NoCredentials.getInstance();
                });

        assertThat(created).hasValue(0);

        details.connection();

        assertThat(created).hasValue(1);
    }

    @Test
    void resolves_the_credentials_again_after_a_failure() {
        AtomicInteger attempts = new AtomicInteger();
        PropertiesBigQueryConnectionDetails details = new PropertiesBigQueryConnectionDetails(
                new BqkitProperties("my-project", null, null), () -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new UncheckedIOException(new IOException("no Application Default Credentials"));
                    }
                    return NoCredentials.getInstance();
                });

        assertThatThrownBy(details::connection).isInstanceOf(UncheckedIOException.class);

        assertThat(details.connection().credentials()).isSameAs(NoCredentials.getInstance());
        assertThat(attempts).hasValue(2);
    }
}
