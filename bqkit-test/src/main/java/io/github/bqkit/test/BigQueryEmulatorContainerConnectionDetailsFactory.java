package io.github.bqkit.test;

import com.google.cloud.NoCredentials;
import io.github.bqkit.autoconfigure.BigQueryConnectionDetails;
import io.github.bqkit.core.BigQueryConnection;
import org.springframework.boot.testcontainers.service.connection.ContainerConnectionDetailsFactory;
import org.springframework.boot.testcontainers.service.connection.ContainerConnectionSource;
import org.testcontainers.containers.BigQueryEmulatorContainer;

/** Lets {@code @ServiceConnection} point the REST and Storage Read clients at a BigQuery emulator container. */
class BigQueryEmulatorContainerConnectionDetailsFactory
        extends ContainerConnectionDetailsFactory<BigQueryEmulatorContainer, BigQueryConnectionDetails> {

    BigQueryEmulatorContainerConnectionDetailsFactory() {
        super(ANY_CONNECTION_NAME, "com.google.cloud.bigquery.BigQuery");
    }

    @Override
    protected BigQueryConnectionDetails getContainerConnectionDetails(ContainerConnectionSource<BigQueryEmulatorContainer> source) {
        return new EmulatorConnectionDetails(source);
    }

    private static final class EmulatorConnectionDetails
            extends ContainerConnectionDetails<BigQueryEmulatorContainer> implements BigQueryConnectionDetails {

        private EmulatorConnectionDetails(ContainerConnectionSource<BigQueryEmulatorContainer> source) {
            super(source);
        }

        @Override
        public BigQueryConnection connection() {
            BigQueryEmulatorContainer container = getContainer();
            return new BigQueryConnection(
                    container.getProjectId(),
                    container.getEmulatorHttpEndpoint(),
                    container.getHost() + ":" + container.getMappedPort(BqkitBigQueryEmulatorContainer.GRPC_PORT),
                    true,
                    NoCredentials.getInstance());
        }
    }
}
