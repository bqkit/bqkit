package io.github.bqkit.autoconfigure;

import com.google.auth.Credentials;
import io.github.bqkit.core.BigQueryConnection;
import io.github.bqkit.core.CredentialsFactory;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Google endpoints with the Application Default Credentials, resolved once when a client first reads them and
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
        return BigQueryConnection.google(properties.projectId(), this::credentials).withLocation(properties.location());
    }

    private synchronized Credentials credentials() {
        if (Objects.isNull(credentials)) {
            credentials = credentialsSupplier.get();
        }
        return credentials;
    }
}
