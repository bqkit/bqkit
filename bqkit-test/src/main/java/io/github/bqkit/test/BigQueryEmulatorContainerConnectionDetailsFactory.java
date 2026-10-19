package io.github.bqkit.test;

import io.github.bqkit.autoconfigure.BigQueryConnectionDetails;
import io.github.bqkit.core.BigQueryConnection;
import org.springframework.boot.testcontainers.service.connection.ContainerConnectionDetailsFactory;
import org.springframework.boot.testcontainers.service.connection.ContainerConnectionSource;

/**
 * Lets {@code @ServiceConnection} point the REST and Storage Read clients at a {@link BqkitBigQueryEmulatorContainer}.
 */
class BigQueryEmulatorContainerConnectionDetailsFactory
        extends ContainerConnectionDetailsFactory<BqkitBigQueryEmulatorContainer, BigQueryConnectionDetails> {

    BigQueryEmulatorContainerConnectionDetailsFactory() {
        super(ANY_CONNECTION_NAME, "com.google.cloud.bigquery.BigQuery");
    }

    @Override
    protected BigQueryConnectionDetails getContainerConnectionDetails(
            ContainerConnectionSource<BqkitBigQueryEmulatorContainer> source) {
        return new EmulatorConnectionDetails(source);
    }

    private static final class EmulatorConnectionDetails
            extends ContainerConnectionDetails<BqkitBigQueryEmulatorContainer> implements BigQueryConnectionDetails {

        private EmulatorConnectionDetails(ContainerConnectionSource<BqkitBigQueryEmulatorContainer> source) {
            super(source);
        }

        @Override
        public BigQueryConnection connection() {
            BqkitBigQueryEmulatorContainer container = getContainer();
            return BigQueryConnection.emulator(
                    container.getProjectId(),
                    container.getEmulatorHttpEndpoint(),
                    container.getHost() + ":" + container.getMappedPort(BqkitBigQueryEmulatorContainer.GRPC_PORT));
        }
    }
}
