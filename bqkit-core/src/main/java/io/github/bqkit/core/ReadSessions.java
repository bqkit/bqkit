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
 * Opens a single-stream read session on a table, billed to the default project, or else to the project of
 * the table, and cancels its stream.
 */
final class ReadSessions {

    private ReadSessions() {
    }

    static ReadSession create(BigQuery bigQuery, BigQueryReadClient readClient, TableId table, DataFormat format) {
        String defaultProject = bigQuery.getOptions().getProjectId();
        String tableProject = Objects.requireNonNullElse(table.getProject(), defaultProject);
        String billingProject = Objects.requireNonNullElse(defaultProject, tableProject);
        return readClient.createReadSession(CreateReadSessionRequest.newBuilder()
                .setParent("projects/" + billingProject)
                .setReadSession(ReadSession.newBuilder()
                        .setTable("projects/%s/datasets/%s/tables/%s"
                                .formatted(tableProject, table.getDataset(), table.getTable()))
                        .setDataFormat(format))
                .setMaxStreamCount(1)
                .build());
    }

    /** Cancels the stream after a failed read: the failure of the read stays the one thrown. */
    static void cancel(ServerStream<?> responses, Throwable failure) {
        try {
            responses.cancel();
        } catch (RuntimeException e) {
            failure.addSuppressed(e);
        }
    }
}
