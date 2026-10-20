package io.github.bqkit.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Properties;
import org.testcontainers.containers.BigQueryEmulatorContainer;

/**
 * BigQuery emulator usable for Avro reads through the Storage Read API.
 *
 * <p>The emulator builds the Avro namespace from the project id, and the {@code test-project} id set by
 * {@link BigQueryEmulatorContainer} contains a hyphen, which its own Avro codec rejects. This container
 * starts the emulator on {@value #PROJECT_ID} and also exposes the gRPC port of the Storage Read API.
 */
public class BqkitBigQueryEmulatorContainer extends BigQueryEmulatorContainer {

    public static final String DEFAULT_IMAGE = defaultImage();
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

    private static String defaultImage() {
        String resource = "bigquery-emulator.properties";
        try (InputStream in = BqkitBigQueryEmulatorContainer.class.getResourceAsStream(resource)) {
            Properties properties = new Properties();
            properties.load(Objects.requireNonNull(in, resource));
            return properties.getProperty("image");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
