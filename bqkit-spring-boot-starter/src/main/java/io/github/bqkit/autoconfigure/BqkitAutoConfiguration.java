package io.github.bqkit.autoconfigure;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import io.github.bqkit.core.BigQueryClients;
import io.github.bqkit.core.QueryArrowReader;
import io.github.bqkit.core.QueryAvroReader;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * BigQuery REST client, Storage Read client, {@link QueryAvroReader} and {@link QueryArrowReader},
 * each replaceable by an application bean.
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
    BigQueryReadClient bigQueryReadClient(BigQueryConnectionDetails details) {
        return BigQueryClients.readClient(details.connection());
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
