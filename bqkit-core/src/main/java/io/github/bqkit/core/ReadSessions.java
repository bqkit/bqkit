package io.github.bqkit.core;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import java.util.Objects;

/**
 * Opens a read session on a table and cancels its streams.
 *
 * <p>The session is created in the connection project, which needs {@code bigquery.readsessions.create} and
 * pays for the read, whichever project owns the table. The project of the table is only the one that holds
 * the data: it is also the billing project when the connection has none. A table without a project is read
 * from the connection project.
 */
final class ReadSessions {

    private ReadSessions() {
    }

    static ReadSession create(BigQuery bigQuery, BigQueryReadClient readClient, TableId table, DataFormat format,
                              int maxStreams) {
        String defaultProject = bigQuery.getOptions().getProjectId();
        String billingProject = Objects.requireNonNull(
                Objects.nonNull(defaultProject) ? defaultProject : table.getProject(),
                "No project to create the read session in: set a project on the connection or on the table");
        String tableProject = Objects.requireNonNullElse(table.getProject(), billingProject);
        return readClient.createReadSession(CreateReadSessionRequest.newBuilder()
                .setParent("projects/" + billingProject)
                .setReadSession(ReadSession.newBuilder()
                        .setTable("projects/%s/datasets/%s/tables/%s"
                                .formatted(tableProject, table.getDataset(), table.getTable()))
                        .setDataFormat(format))
                .setMaxStreamCount(maxStreams)
                .build());
    }

    /** Cancels the stream after a failed read: the failure of the read stays the one thrown. */
    static int requirePositive(int maxStreams) {
        if (maxStreams < 1) {
            throw new IllegalArgumentException("maxStreams must be at least 1, was " + maxStreams);
        }
        return maxStreams;
    }

    static void cancel(ServerStream<?> responses, Throwable failure) {
        try {
            responses.cancel();
        } catch (RuntimeException e) {
            failure.addSuppressed(e);
        }
    }
}
