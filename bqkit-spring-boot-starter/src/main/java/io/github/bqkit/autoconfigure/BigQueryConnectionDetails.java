package io.github.bqkit.autoconfigure;

import io.github.bqkit.core.BigQueryConnection;
import org.springframework.boot.autoconfigure.service.connection.ConnectionDetails;

/**
 * Connection to BigQuery. Defined from {@link BqkitProperties} by default, or by a
 * {@code @ServiceConnection} container in tests.
 */
@FunctionalInterface
public interface BigQueryConnectionDetails extends ConnectionDetails {

    BigQueryConnection connection();
}
