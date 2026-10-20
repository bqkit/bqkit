package io.github.bqkit.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * BigQuery settings under {@code bqkit.bigquery}.
 *
 * @param projectId Project billed for the queries; the Google default project when absent.
 * @param impersonateServiceAccount Service account impersonated with the Application Default Credentials, if any.
 * @param location Location of the query jobs, such as EU or europe-west1; BigQuery picks it from the tables of the
 *     query when absent.
 * @param maxReadStreams Streams of a read session read at the same time; more than 1 hands the rows in no order.
 */
@ConfigurationProperties("bqkit.bigquery")
public record BqkitProperties(String projectId, String impersonateServiceAccount, String location,
                              @DefaultValue("1") int maxReadStreams) {
}
