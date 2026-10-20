package io.github.bqkit.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ResourceBundle;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

class BqkitBigQueryEmulatorContainerTest {

    @Test
    void default_image_is_the_one_set_by_the_build() {
        String expected = ResourceBundle.getBundle("io.github.bqkit.test.bigquery-emulator").getString("image");

        assertThat(BqkitBigQueryEmulatorContainer.DEFAULT_IMAGE)
                .isEqualTo(expected)
                .doesNotContain("${")
                .startsWith("ghcr.io/goccy/bigquery-emulator:");
    }

    @Test
    void runs_an_image_from_a_mirror_declared_as_a_substitute_for_the_emulator() {
        DockerImageName mirror = DockerImageName.parse("registry.example.com/goccy/bigquery-emulator:0.8.1")
                .asCompatibleSubstituteFor("ghcr.io/goccy/bigquery-emulator");

        try (BqkitBigQueryEmulatorContainer emulator = new BqkitBigQueryEmulatorContainer(mirror)) {
            assertThat(emulator.getProjectId()).isEqualTo(BqkitBigQueryEmulatorContainer.PROJECT_ID);
            assertThat(emulator.getExposedPorts()).containsExactly(
                    BqkitBigQueryEmulatorContainer.HTTP_PORT, BqkitBigQueryEmulatorContainer.GRPC_PORT);
        }
    }

    @Test
    void rejects_a_mirror_image_not_declared_as_a_substitute_for_the_emulator() {
        assertThatThrownBy(() -> new BqkitBigQueryEmulatorContainer(
                "registry.example.com/goccy/bigquery-emulator:0.8.1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ghcr.io/goccy/bigquery-emulator");
    }
}
