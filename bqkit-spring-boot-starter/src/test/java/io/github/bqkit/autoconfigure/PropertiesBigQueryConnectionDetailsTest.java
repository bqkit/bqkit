package io.github.bqkit.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PropertiesBigQueryConnectionDetailsTest {

    @Test
    void shares_the_credentials_between_the_clients() {
        AtomicInteger created = new AtomicInteger();
        PropertiesBigQueryConnectionDetails details = new PropertiesBigQueryConnectionDetails(
                new BqkitProperties("my-project", null), () -> {
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

        new PropertiesBigQueryConnectionDetails(new BqkitProperties("my-project", null), () -> {
            created.incrementAndGet();
            return NoCredentials.getInstance();
        });

        assertThat(created).hasValue(0);
    }
}
