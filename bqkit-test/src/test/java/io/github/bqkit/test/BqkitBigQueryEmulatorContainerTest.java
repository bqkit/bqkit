package io.github.bqkit.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ResourceBundle;
import org.junit.jupiter.api.Test;

class BqkitBigQueryEmulatorContainerTest {

    @Test
    void default_image_is_the_one_set_by_the_build() {
        String expected = ResourceBundle.getBundle("io.github.bqkit.test.bigquery-emulator").getString("image");

        assertThat(BqkitBigQueryEmulatorContainer.DEFAULT_IMAGE).isEqualTo(expected);
    }
}
