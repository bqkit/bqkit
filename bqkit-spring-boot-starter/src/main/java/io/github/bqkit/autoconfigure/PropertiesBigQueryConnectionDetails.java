package io.github.bqkit.autoconfigure;

import io.github.bqkit.core.BigQueryConnection;
import io.github.bqkit.core.CredentialsFactory;

/** Google endpoints with the Application Default Credentials, resolved only when the clients are built. */
final class PropertiesBigQueryConnectionDetails implements BigQueryConnectionDetails {

    private final BqkitProperties properties;

    PropertiesBigQueryConnectionDetails(BqkitProperties properties) {
        this.properties = properties;
    }

    @Override
    public BigQueryConnection connection() {
        return new BigQueryConnection(properties.projectId(), null, null, false,
                CredentialsFactory.create(properties.impersonateServiceAccount()));
    }
}
