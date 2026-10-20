package io.github.bqkit.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.spring.autoconfigure.bigquery.GcpBigQueryAutoConfiguration;
import com.google.cloud.spring.autoconfigure.core.GcpContextAutoConfiguration;
import io.github.bqkit.core.BigQueryConnection;
import io.github.bqkit.core.QueryArrowReader;
import io.github.bqkit.core.QueryAvroReader;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class BqkitAutoConfigurationTest {

    private static final BigQueryConnectionDetails EMULATOR_DETAILS = () -> BigQueryConnection.emulator(
            "testproject", "http://localhost:9050", "localhost:9060");

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(BqkitAutoConfiguration.class));

    @Test
    void connection_details_come_from_properties_when_none_is_defined() {
        runner.withPropertyValues(
                        "bqkit.bigquery.project-id=my-project",
                        "bqkit.bigquery.impersonate-service-account=reader@my-project.iam.gserviceaccount.com",
                        "bqkit.bigquery.location=EU")
                .withBean(BigQuery.class, () -> mock(BigQuery.class))
                .withBean(BigQueryReadClient.class, () -> mock(BigQueryReadClient.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(BigQueryConnectionDetails.class)
                            .getBean(BigQueryConnectionDetails.class)
                            .isInstanceOf(PropertiesBigQueryConnectionDetails.class);
                    assertThat(context.getBean(BqkitProperties.class)).isEqualTo(new BqkitProperties(
                            "my-project", "reader@my-project.iam.gserviceaccount.com", "EU", 1));
                });
    }

    @Test
    void clients_and_reader_are_built_from_the_connection_details() {
        runner.withBean(BigQueryConnectionDetails.class, () -> EMULATOR_DETAILS)
                .run(context -> {
                    assertThat(context).hasSingleBean(BigQueryConnectionDetails.class)
                            .hasSingleBean(BigQueryReadClient.class)
                            .hasSingleBean(QueryAvroReader.class)
                            .hasSingleBean(QueryArrowReader.class);
                    BigQuery bigQuery = context.getBean(BigQuery.class);
                    assertThat(bigQuery.getOptions().getProjectId()).isEqualTo("testproject");
                    assertThat(bigQuery.getOptions().getHost()).isEqualTo("http://localhost:9050");
                });
    }

    @Test
    void readers_read_as_many_streams_at_once_as_the_property_says() {
        BigQueryReadClient readClient = mock(BigQueryReadClient.class);
        when(readClient.createReadSession(any(CreateReadSessionRequest.class)))
                .thenThrow(new IllegalStateException("no session"));
        runner.withPropertyValues("bqkit.bigquery.max-read-streams=4")
                .withBean(BigQueryConnectionDetails.class, () -> EMULATOR_DETAILS)
                .withBean(BigQueryReadClient.class, () -> readClient)
                .run(context -> {
                    TableId table = TableId.of("shop", "products");
                    assertThatThrownBy(() -> context.getBean(QueryAvroReader.class).readTable(table));
                    assertThatThrownBy(() -> context.getBean(QueryArrowReader.class).readTable(table, batch -> { }));
                    ArgumentCaptor<CreateReadSessionRequest> requests =
                            ArgumentCaptor.forClass(CreateReadSessionRequest.class);
                    verify(readClient, times(2)).createReadSession(requests.capture());
                    assertThat(requests.getAllValues()).extracting(CreateReadSessionRequest::getMaxStreamCount)
                            .containsExactly(4, 4);
                });
    }

    @Test
    void backs_off_when_the_application_defines_its_own_bigquery_client() {
        BigQuery custom = mock(BigQuery.class);
        runner.withBean(BigQueryConnectionDetails.class, () -> EMULATOR_DETAILS)
                .withBean(BigQuery.class, () -> custom)
                .run(context -> assertThat(context.getBean(BigQuery.class)).isSameAs(custom));
    }

    @Test
    void arrow_reader_backs_off_without_touching_the_avro_reader() {
        QueryArrowReader custom = mock(QueryArrowReader.class);
        runner.withBean(BigQueryConnectionDetails.class, () -> EMULATOR_DETAILS)
                .withBean(QueryArrowReader.class, () -> custom)
                .run(context -> {
                    assertThat(context.getBean(QueryArrowReader.class)).isSameAs(custom);
                    assertThat(context).hasSingleBean(QueryAvroReader.class);
                });
    }

    @Test
    void reuses_the_bigquery_client_of_spring_cloud_gcp_when_both_are_on_the_classpath() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(BqkitAutoConfiguration.class,
                        GcpBigQueryAutoConfiguration.class, GcpContextAutoConfiguration.class))
                .withPropertyValues("spring.cloud.gcp.project-id=gcp-project",
                        "spring.cloud.gcp.bigquery.dataset-name=shop")
                .withBean(CredentialsProvider.class,
                        () -> () -> GoogleCredentials.create(new AccessToken("token", null)))
                .withBean(BigQueryConnectionDetails.class, () -> EMULATOR_DETAILS)
                .run(context -> {
                    assertThat(context).hasNotFailed()
                            .hasSingleBean(BigQuery.class)
                            .hasSingleBean(QueryAvroReader.class)
                            .hasSingleBean(QueryArrowReader.class);
                    assertThat(context.getBean(BigQuery.class).getOptions().getProjectId())
                            .isEqualTo("gcp-project");
                });
    }
}
