package io.github.bqkit.core;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import java.util.Objects;

/** Runs a query job to completion and returns the table it wrote to. */
final class QueryJobs {

    private QueryJobs() {
    }

    static QueryJobConfiguration anonymous(String sql) {
        return QueryJobConfiguration.newBuilder(sql).build();
    }

    /** Writes the result to {@code destination} (truncated first) instead of the anonymous result table. */
    static QueryJobConfiguration into(String sql, TableId destination) {
        return QueryJobConfiguration.newBuilder(sql)
                .setDestinationTable(destination)
                .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
                .build();
    }

    /** Cancels the job if the wait is interrupted, so that it does not keep running and billing. */
    static TableId run(BigQuery bigQuery, QueryJobConfiguration configuration) {
        Job submitted = bigQuery.create(JobInfo.of(configuration));
        Job job;
        try {
            job = submitted.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            IllegalStateException interrupted = new IllegalStateException(e);
            try {
                submitted.cancel();
            } catch (RuntimeException cancelFailure) {
                interrupted.addSuppressed(cancelFailure);
            }
            throw interrupted;
        }
        if (Objects.isNull(job)) {
            throw new IllegalStateException("BigQuery job not found");
        }
        if (Objects.nonNull(job.getStatus().getError())) {
            throw new IllegalStateException("BigQuery job failed: " + job.getStatus().getError());
        }
        TableId destination = ((QueryJobConfiguration) job.getConfiguration()).getDestinationTable();
        if (Objects.isNull(destination)) {
            throw new IllegalStateException(
                    "BigQuery job wrote no destination table: only a SELECT statement can be read");
        }
        return destination;
    }
}
