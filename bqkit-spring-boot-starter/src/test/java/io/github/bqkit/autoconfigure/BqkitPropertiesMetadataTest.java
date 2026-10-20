package io.github.bqkit.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class BqkitPropertiesMetadataTest {

    private static final String METADATA = "META-INF/spring-configuration-metadata.json";

    @Test
    void the_ide_knows_the_bqkit_properties() throws IOException {
        DocumentContext metadata = JsonPath.parse(bqkitMetadata());

        assertThat(metadata.<List<String>>read("$.properties[*].name"))
                .containsExactlyInAnyOrder("bqkit.bigquery.project-id", "bqkit.bigquery.impersonate-service-account",
                        "bqkit.bigquery.location");
        assertThat(metadata.<List<String>>read("$.properties[?(@.name == 'bqkit.bigquery.project-id')].description"))
                .singleElement().asString().containsIgnoringCase("project billed for the queries");
        assertThat(metadata.<List<String>>read(
                "$.properties[?(@.name == 'bqkit.bigquery.impersonate-service-account')].description"))
                .singleElement().asString().containsIgnoringCase("service account impersonated");
        assertThat(metadata.<List<String>>read("$.properties[?(@.name == 'bqkit.bigquery.location')].description"))
                .singleElement().asString().containsIgnoringCase("location of the query jobs");
    }

    private static String bqkitMetadata() throws IOException {
        for (URL url : Collections.list(BqkitPropertiesMetadataTest.class.getClassLoader().getResources(METADATA))) {
            try (InputStream in = url.openStream()) {
                String json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                if (json.contains("\"bqkit.bigquery\"")) {
                    return json;
                }
            }
        }
        throw new AssertionError("No " + METADATA + " describes bqkit.bigquery");
    }
}
