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
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * BigQuery REST client, Storage Read client, {@link QueryAvroReader} and {@link QueryArrowReader},
 * each replaceable by an application bean. The Storage Read client takes the credentials of the
 * BigQuery client, so that the query and the read run with the same identity.
 */
@AutoConfiguration(afterName = "com.google.cloud.spring.autoconfigure.bigquery.GcpBigQueryAutoConfiguration")
@ConditionalOnClass({BigQuery.class, BigQueryReadClient.class})
@EnableConfigurationProperties(BqkitProperties.class)
public class BqkitAutoConfiguration {

    private static final Log LOGGER = LogFactory.getLog(BqkitAutoConfiguration.class);

    private BigQuery builtBigQuery;

    @Bean
    @ConditionalOnMissingBean
    BigQueryConnectionDetails bigQueryConnectionDetails(BqkitProperties properties) {
        return new PropertiesBigQueryConnectionDetails(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    BigQuery bigQuery(BigQueryConnectionDetails details) {
        builtBigQuery = BigQueryClients.bigQuery(details.connection());
        return builtBigQuery;
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    BigQueryReadClient bigQueryReadClient(
            BigQueryConnectionDetails details, BigQuery bigQuery, BqkitProperties properties) {
        BigQueryConnection connection = details.connection();
        BigQueryOptions options = bigQuery.getOptions();
        if (bigQuery != builtBigQuery) {
            logForeignClient(options, properties);
        }
        return BigQueryClients.readClient(connection.withCredentials(options.getCredentials()));
    }

    private static void logForeignClient(BigQueryOptions options, BqkitProperties properties) {
        boolean propertiesSet = Stream.of(properties.projectId(), properties.impersonateServiceAccount())
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
}
