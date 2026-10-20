package io.github.bqkit.autoconfigure;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import io.github.bqkit.core.BigQueryClients;
import io.github.bqkit.core.BigQueryConnection;
import io.github.bqkit.core.QueryArrowReader;
import io.github.bqkit.core.QueryAvroReader;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * BigQuery REST client, Storage Read client, {@link QueryAvroReader} and {@link QueryArrowReader},
 * each replaceable by an application bean. The Storage Read client takes the credentials of the
 * BigQuery client, so that the query and the read run with the same identity.
 */
@AutoConfiguration(afterName = "com.google.cloud.spring.autoconfigure.bigquery.GcpBigQueryAutoConfiguration")
@ConditionalOnClass({BigQuery.class, BigQueryReadClient.class})
@EnableConfigurationProperties(BqkitProperties.class)
public class BqkitAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    BigQueryConnectionDetails bigQueryConnectionDetails(BqkitProperties properties) {
        return new PropertiesBigQueryConnectionDetails(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    BigQuery bigQuery(BigQueryConnectionDetails details) {
        return BigQueryClients.bigQuery(details.connection());
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    BigQueryReadClient bigQueryReadClient(BigQueryConnectionDetails details, BigQuery bigQuery) {
        return BigQueryClients.readClient(
                details.connection().withCredentials(bigQuery.getOptions().getCredentials()));
    }

    @Bean
    @ConditionalOnMissingBean
    QueryAvroReader queryAvroReader(BigQuery bigQuery, BigQueryReadClient readClient,
                                    BqkitProperties properties) {
        return new QueryAvroReader(bigQuery, readClient, properties.maxReadStreams());
    }

    @Bean
    @ConditionalOnMissingBean
    QueryArrowReader queryArrowReader(BigQuery bigQuery, BigQueryReadClient readClient,
                                      BqkitProperties properties) {
        return new QueryArrowReader(bigQuery, readClient, properties.maxReadStreams());
    }

    /**
     * Evaluated before the beans of bqkit are registered, so it only matches a {@link BigQuery} client built
     * elsewhere, with a Storage Read client that bqkit builds.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBean(BigQuery.class)
    @ConditionalOnMissingBean(BigQueryReadClient.class)
    static class ForeignBigQueryConfiguration {

        private static final Log LOGGER = LogFactory.getLog(BqkitAutoConfiguration.class);

        ForeignBigQueryConfiguration(BigQuery bigQuery, BqkitProperties properties,
                                     BigQueryConnectionDetails details) {
            BigQueryOptions options = bigQuery.getOptions();
            BigQueryConnection connection = details.connection();
            if (connection.plaintext() && !Objects.equals(options.getHost(), connection.restHost())) {
                LOGGER.warn("bqkit reads from the emulator at " + connection.grpcEndpoint() + " but queries run on "
                        + options.getHost() + " with the BigQuery client bean that bqkit did not build: exclude the "
                        + "auto-configuration that builds it, such as GcpBigQueryAutoConfiguration, in tests");
            }
            boolean propertiesSet = Stream.of(properties.projectId(), properties.impersonateServiceAccount(),
                            properties.location())
                    .anyMatch(value -> Objects.nonNull(value) && !value.isBlank());
            if (propertiesSet) {
                LOGGER.warn("bqkit.bigquery.* do not apply: the BigQuery client bean was not built by bqkit; queries "
                        + "and Storage Read sessions run with its project " + options.getProjectId()
                        + " and its credentials");
            } else {
                LOGGER.info("bqkit uses the BigQuery client bean of the context, project " + options.getProjectId()
                        + "; the Storage Read client takes its credentials");
            }
        }
    }
}
