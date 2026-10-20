package io.github.bqkit.core;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/** Builds the REST and Storage Read clients from the same {@link BigQueryConnection}. */
public final class BigQueryClients {

    private BigQueryClients() {
    }

    public static BigQuery bigQuery(BigQueryConnection connection) {
        return BigQueryOptions.newBuilder()
                .setProjectId(connection.projectId())
                .setCredentials(connection.credentials())
                .setHost(connection.restHost())
                .setLocation(connection.location())
                .build()
                .getService();
    }

    /** The caller owns the returned client and closes it, which shuts down its gRPC channel. */
    public static BigQueryReadClient readClient(BigQueryConnection connection) {
        Credentials credentials = connection.credentials();
        BigQueryReadSettings.Builder settings = BigQueryReadSettings.newBuilder()
                .setCredentialsProvider(credentials instanceof NoCredentials
                        ? NoCredentialsProvider.create()
                        : FixedCredentialsProvider.create(credentials));
        if (Objects.nonNull(connection.grpcEndpoint())) {
            InstantiatingGrpcChannelProvider.Builder channel = BigQueryReadSettings
                    .defaultGrpcTransportProviderBuilder()
                    .setEndpoint(connection.grpcEndpoint());
            if (connection.plaintext()) {
                channel.setChannelConfigurator(ManagedChannelBuilder::usePlaintext);
            }
            settings.setTransportChannelProvider(channel.build());
        }
        try {
            return BigQueryReadClient.create(settings.build());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
