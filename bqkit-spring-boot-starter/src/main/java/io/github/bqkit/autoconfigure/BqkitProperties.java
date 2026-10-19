package io.github.bqkit.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * BigQuery settings under {@code bqkit.bigquery}.
 *
 * @param projectId project billed for the queries; the Google default project when absent
 * @param impersonateServiceAccount service account impersonated with the Application Default Credentials, if any
 */
@ConfigurationProperties("bqkit.bigquery")
public record BqkitProperties(String projectId, String impersonateServiceAccount) {
}
