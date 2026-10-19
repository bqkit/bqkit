package io.github.bqkit.autoconfigure;

import com.google.auth.Credentials;
import io.github.bqkit.core.BigQueryConnection;
import io.github.bqkit.core.CredentialsFactory;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Google endpoints with the Application Default Credentials, resolved once when the first client is built and
 * shared by both clients.
 */
final class PropertiesBigQueryConnectionDetails implements BigQueryConnectionDetails {

    private final BqkitProperties properties;
    private final Supplier<Credentials> credentialsSupplier;
    private Credentials credentials;

    PropertiesBigQueryConnectionDetails(BqkitProperties properties) {
        this(properties, () -> CredentialsFactory.create(properties.impersonateServiceAccount()));
    }

    PropertiesBigQueryConnectionDetails(BqkitProperties properties, Supplier<Credentials> credentialsSupplier) {
        this.properties = properties;
        this.credentialsSupplier = credentialsSupplier;
    }

    @Override
    public BigQueryConnection connection() {
        return new BigQueryConnection(properties.projectId(), null, null, false, credentials());
    }

    private synchronized Credentials credentials() {
        if (Objects.isNull(credentials)) {
            credentials = credentialsSupplier.get();
        }
        return credentials;
    }
}
