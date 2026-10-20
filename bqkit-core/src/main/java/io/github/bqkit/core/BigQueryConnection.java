package io.github.bqkit.core;

import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Where and how to reach BigQuery: the Google endpoints, or an emulator.
 */
public final class BigQueryConnection {

    private final String projectId;
    private final String restHost;
    private final String grpcEndpoint;
    private final boolean plaintext;
    private final Supplier<Credentials> credentials;
    private final String location;

    private BigQueryConnection(String projectId, String restHost, String grpcEndpoint, boolean plaintext,
            Supplier<Credentials> credentials, String location) {
        this.projectId = projectId;
        this.restHost = restHost;
        this.grpcEndpoint = grpcEndpoint;
        this.plaintext = plaintext;
        this.credentials = credentials;
        this.location = location;
    }

    /**
     * Connection to the Google endpoints over TLS.
     *
     * @param projectId   project that runs the jobs and owns the read sessions
     * @param credentials credentials shared by the REST and gRPC clients
     * @return the connection
     */
    public static BigQueryConnection google(String projectId, Credentials credentials) {
        return google(projectId, () -> credentials);
    }

    /**
     * Connection to the Google endpoints over TLS, with credentials resolved each time they are read.
     *
     * @param projectId   project that runs the jobs and owns the read sessions
     * @param credentials supplies the credentials shared by the REST and gRPC clients
     * @return the connection
     */
    public static BigQueryConnection google(String projectId, Supplier<Credentials> credentials) {
        return new BigQueryConnection(projectId, null, null, false, credentials, null);
    }

    /**
     * Connection to an emulator, in plaintext and without credentials.
     *
     * @param projectId    project that runs the jobs and owns the read sessions
     * @param restHost     REST host, such as {@code http://localhost:9050}
     * @param grpcEndpoint Storage Read API endpoint as {@code host:port}
     * @return the connection
     */
    public static BigQueryConnection emulator(String projectId, String restHost, String grpcEndpoint) {
        return new BigQueryConnection(projectId, restHost, grpcEndpoint, true, NoCredentials::getInstance, null);
    }

    /**
     * Same connection, with the jobs run in {@code location}.
     *
     * @param location location of the jobs, such as {@code EU} or {@code europe-west1}; {@code null} or blank for
     *                 none
     * @return the connection in that location
     */
    public BigQueryConnection withLocation(String location) {
        return new BigQueryConnection(projectId, restHost, grpcEndpoint, plaintext, credentials,
                Objects.isNull(location) || location.isBlank() ? null : location);
    }

    /**
     * Project that runs the jobs and owns the read sessions.
     *
     * @return the project id
     */
    public String projectId() {
        return projectId;
    }

    /**
     * REST host.
     *
     * @return the host, {@code null} for the Google default endpoint
     */
    public String restHost() {
        return restHost;
    }

    /**
     * Storage Read API endpoint as {@code host:port}.
     *
     * @return the endpoint, {@code null} for the Google default endpoint
     */
    public String grpcEndpoint() {
        return grpcEndpoint;
    }

    /**
     * Whether gRPC runs without TLS, for an emulator.
     *
     * @return {@code true} for an emulator
     */
    public boolean plaintext() {
        return plaintext;
    }

    /**
     * Credentials shared by the REST and gRPC clients.
     *
     * @return the credentials
     */
    public Credentials credentials() {
        return credentials.get();
    }

    /**
     * Same endpoints with other credentials, such as those of a BigQuery client built elsewhere.
     *
     * @param credentials credentials shared by the REST and gRPC clients
     * @return the connection
     */
    public BigQueryConnection withCredentials(Credentials credentials) {
        return new BigQueryConnection(projectId, restHost, grpcEndpoint, plaintext, () -> credentials, location);
    }

    /**
     * Location of the jobs.
     *
     * @return the location, {@code null} to let BigQuery pick it from the tables of the query
     */
    public String location() {
        return location;
    }
}
