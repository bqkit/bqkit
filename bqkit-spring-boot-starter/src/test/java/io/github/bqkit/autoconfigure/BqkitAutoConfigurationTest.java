package io.github.bqkit.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.spring.autoconfigure.bigquery.GcpBigQueryAutoConfiguration;
import com.google.cloud.spring.autoconfigure.core.GcpContextAutoConfiguration;
import io.github.bqkit.core.BigQueryConnection;
import io.github.bqkit.core.QueryArrowReader;
import io.github.bqkit.core.QueryAvroReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith(OutputCaptureExtension.class)
class BqkitAutoConfigurationTest {

    private static final BigQueryConnectionDetails EMULATOR_DETAILS = () -> BigQueryConnection.emulator(
            "testproject", "http://localhost:9050", "localhost:9060");

    private static final GoogleCredentials GCP_CREDENTIALS = GoogleCredentials.create(new AccessToken("token", null));

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
        BigQuery custom = customBigQuery(GCP_CREDENTIALS);
        runner.withBean(BigQueryConnectionDetails.class, () -> EMULATOR_DETAILS)
                .withBean(BigQuery.class, () -> custom)
                .run(context -> {
                    assertThat(context.getBean(BigQuery.class)).isSameAs(custom);
                    assertThat(context.getBean(BigQueryReadClient.class).getSettings().getCredentialsProvider()
                            .getCredentials()).isSameAs(GCP_CREDENTIALS);
                });
    }

    @Test
    void readers_share_the_bigquery_and_storage_read_clients_of_the_context() {
        runner.withBean(BigQueryConnectionDetails.class, () -> EMULATOR_DETAILS)
                .run(context -> {
                    BigQuery bigQuery = context.getBean(BigQuery.class);
                    BigQueryReadClient readClient = context.getBean(BigQueryReadClient.class);
                    assertThat(context.getBean(QueryAvroReader.class))
                            .extracting("bigQuery", "readClient").containsExactly(bigQuery, readClient);
                    assertThat(context.getBean(QueryArrowReader.class))
                            .extracting("bigQuery", "readClient").containsExactly(bigQuery, readClient);
                });
    }

    @Test
    void says_nothing_when_the_bigquery_client_is_built_from_the_connection_details(CapturedOutput output) {
        runner.withBean(BigQueryConnectionDetails.class, () -> EMULATOR_DETAILS)
                .run(context -> assertThat(output).doesNotContain("bqkit"));
    }

    @Test
    void warns_that_bqkit_properties_do_not_apply_to_a_bigquery_client_built_elsewhere(CapturedOutput output) {
        BigQuery custom = customBigQuery(GCP_CREDENTIALS);
        runner.withPropertyValues("bqkit.bigquery.project-id=testproject")
                .withBean(BigQueryConnectionDetails.class, () -> EMULATOR_DETAILS)
                .withBean(BigQuery.class, () -> custom)
                .run(context -> assertThat(output)
                        .contains("WARN")
                        .contains("custom-project")
                        .contains("bqkit.bigquery.* do not apply"));
    }

    @Test
    void warns_that_the_location_does_not_apply_to_a_bigquery_client_built_elsewhere(CapturedOutput output) {
        BigQuery custom = customBigQuery(NoCredentials.getInstance());
        runner.withPropertyValues("bqkit.bigquery.location=EU")
                .withBean(BigQueryConnectionDetails.class, () -> EMULATOR_DETAILS)
                .withBean(BigQuery.class, () -> custom)
                .run(context -> assertThat(output)
                        .contains("WARN")
                        .contains("bqkit.bigquery.* do not apply"));
    }

    @Test
    void does_not_warn_about_blank_bqkit_properties_with_a_bigquery_client_built_elsewhere(CapturedOutput output) {
        BigQuery custom = customBigQuery(NoCredentials.getInstance());
        runner.withPropertyValues("bqkit.bigquery.location= ")
                .withBean(BigQueryConnectionDetails.class, () -> EMULATOR_DETAILS)
                .withBean(BigQuery.class, () -> custom)
                .run(context -> assertThat(output)
                        .contains("INFO")
                        .doesNotContain("WARN"));
    }

    @Test
    void tells_which_bigquery_client_is_used_when_it_comes_from_elsewhere(CapturedOutput output) {
        BigQuery custom = customBigQuery(NoCredentials.getInstance());
        runner.withBean(BigQueryConnectionDetails.class, () -> EMULATOR_DETAILS)
                .withBean(BigQuery.class, () -> custom)
                .run(context -> assertThat(output)
                        .contains("INFO")
                        .doesNotContain("WARN")
                        .contains("custom-project"));
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
                .withBean(CredentialsProvider.class, () -> () -> GCP_CREDENTIALS)
                .withBean(BigQueryConnectionDetails.class, () -> EMULATOR_DETAILS)
                .run(context -> {
                    assertThat(context).hasNotFailed()
                            .hasSingleBean(BigQuery.class)
                            .hasSingleBean(QueryAvroReader.class)
                            .hasSingleBean(QueryArrowReader.class);
                    assertThat(context.getBean(BigQuery.class).getOptions().getProjectId())
                            .isEqualTo("gcp-project");
                    assertThat(context.getBean(BigQueryReadClient.class).getSettings().getCredentialsProvider()
                            .getCredentials()).isSameAs(GCP_CREDENTIALS);
                });
    }

    @Test
    void starts_with_spring_cloud_gcp_without_application_default_credentials() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(BqkitAutoConfiguration.class,
                        GcpBigQueryAutoConfiguration.class, GcpContextAutoConfiguration.class))
                .withPropertyValues("spring.cloud.gcp.project-id=gcp-project",
                        "spring.cloud.gcp.bigquery.dataset-name=shop")
                .withBean(CredentialsProvider.class, () -> () -> GCP_CREDENTIALS)
                .withBean(BigQueryConnectionDetails.class, () -> new PropertiesBigQueryConnectionDetails(
                        new BqkitProperties(null, null, null, 1), () -> {
                            throw new UncheckedIOException(new IOException("no Application Default Credentials"));
                        }))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(BigQueryReadClient.class).getSettings().getCredentialsProvider()
                            .getCredentials()).isSameAs(GCP_CREDENTIALS);
                });
    }

    private static BigQuery customBigQuery(Credentials credentials) {
        return BigQueryOptions.newBuilder()
                .setProjectId("custom-project")
                .setCredentials(credentials)
                .build()
                .getService();
    }
}
