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
 * Opens a single-stream read session on a table, in the project of the table or else the default one, and
 * cancels its stream.
 */
final class ReadSessions {

    private ReadSessions() {
    }

    static ReadSession create(BigQuery bigQuery, BigQueryReadClient readClient, TableId table, DataFormat format) {
        String project = Objects.requireNonNullElse(table.getProject(), bigQuery.getOptions().getProjectId());
        return readClient.createReadSession(CreateReadSessionRequest.newBuilder()
                .setParent("projects/" + project)
                .setReadSession(ReadSession.newBuilder()
                        .setTable("projects/%s/datasets/%s/tables/%s"
                                .formatted(project, table.getDataset(), table.getTable()))
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
