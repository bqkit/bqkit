package io.github.bqkit.test;

import org.testcontainers.containers.BigQueryEmulatorContainer;

/**
 * BigQuery emulator usable for Avro reads through the Storage Read API.
 *
 * <p>The emulator builds the Avro namespace from the project id, and the {@code test-project} id set by
 * {@link BigQueryEmulatorContainer} contains a hyphen, which its own Avro codec rejects. This container
 * starts the emulator on {@value #PROJECT_ID} and also exposes the gRPC port of the Storage Read API.
 */
public class BqkitBigQueryEmulatorContainer extends BigQueryEmulatorContainer {

    public static final String DEFAULT_IMAGE = "ghcr.io/goccy/bigquery-emulator:0.8.1";
    public static final String PROJECT_ID = "testproject";
    public static final int HTTP_PORT = 9050;
    public static final int GRPC_PORT = 9060;

    public BqkitBigQueryEmulatorContainer() {
        this(DEFAULT_IMAGE);
    }

    public BqkitBigQueryEmulatorContainer(String image) {
        super(image);
        withCommand("--project", PROJECT_ID);
        withExposedPorts(HTTP_PORT, GRPC_PORT);
    }

    @Override
    public String getProjectId() {
        return PROJECT_ID;
    }
}
