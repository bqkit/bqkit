package io.github.bqkit.test;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BqkitBigQueryEmulatorContainerTest {

    @Test
    void default_image_is_the_one_set_by_the_build() {
        assertThat(BqkitBigQueryEmulatorContainer.DEFAULT_IMAGE)
                .isNotBlank()
                .isNotEqualTo("${bigquery-emulator.image}");
    }
}
