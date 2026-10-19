package io.github.bqkit.core;

import com.google.auth.Credentials;

/**
 * Where and how to reach BigQuery.
 *
 * @param projectId    project that runs the jobs and owns the read sessions
 * @param restHost     REST host, {@code null} for the Google default endpoint
 * @param grpcEndpoint Storage Read API endpoint as {@code host:port}, {@code null} for the Google default endpoint
 * @param plaintext    gRPC without TLS, for an emulator
 * @param credentials  credentials shared by the REST and gRPC clients
 */
public record BigQueryConnection(
        String projectId,
        String restHost,
        String grpcEndpoint,
        boolean plaintext,
        Credentials credentials) {
}
