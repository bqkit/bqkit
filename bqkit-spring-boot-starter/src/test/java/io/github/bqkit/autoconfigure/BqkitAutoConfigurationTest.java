package io.github.bqkit.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import io.github.bqkit.core.BigQueryConnection;
import io.github.bqkit.core.QueryAvroReader;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class BqkitAutoConfigurationTest {

    private static final BigQueryConnectionDetails EMULATOR_DETAILS = () -> new BigQueryConnection(
            "testproject", "http://localhost:9050", "localhost:9060", true, NoCredentials.getInstance());

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(BqkitAutoConfiguration.class));

    @Test
    void connection_details_come_from_properties_when_none_is_defined() {
        runner.withPropertyValues(
                        "bqkit.bigquery.project-id=my-project",
                        "bqkit.bigquery.impersonate-service-account=reader@my-project.iam.gserviceaccount.com")
                .withBean(BigQuery.class, () -> mock(BigQuery.class))
                .withBean(BigQueryReadClient.class, () -> mock(BigQueryReadClient.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(BigQueryConnectionDetails.class)
                            .getBean(BigQueryConnectionDetails.class)
                            .isInstanceOf(PropertiesBigQueryConnectionDetails.class);
                    assertThat(context.getBean(BqkitProperties.class)).isEqualTo(new BqkitProperties(
                            "my-project", "reader@my-project.iam.gserviceaccount.com"));
                });
    }

    @Test
    void clients_and_reader_are_built_from_the_connection_details() {
        runner.withBean(BigQueryConnectionDetails.class, () -> EMULATOR_DETAILS)
                .run(context -> {
                    assertThat(context).hasSingleBean(BigQueryConnectionDetails.class)
                            .hasSingleBean(BigQueryReadClient.class)
                            .hasSingleBean(QueryAvroReader.class);
                    BigQuery bigQuery = context.getBean(BigQuery.class);
                    assertThat(bigQuery.getOptions().getProjectId()).isEqualTo("testproject");
                    assertThat(bigQuery.getOptions().getHost()).isEqualTo("http://localhost:9050");
                });
    }

    @Test
    void backs_off_when_the_application_defines_its_own_bigquery_client() {
        BigQuery custom = mock(BigQuery.class);
        runner.withBean(BigQueryConnectionDetails.class, () -> EMULATOR_DETAILS)
                .withBean(BigQuery.class, () -> custom)
                .run(context -> assertThat(context.getBean(BigQuery.class)).isSameAs(custom));
    }
}
