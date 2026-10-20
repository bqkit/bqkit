package io.github.bqkit.core;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import java.util.Map;
import java.util.Objects;
import org.testcontainers.containers.BigQueryEmulatorContainer;

final class Emulator {

    static final String IMAGE = System.getProperty("bigquery-emulator.image");
    static final String PROJECT_ID = "testproject";
    static final int GRPC_PORT = 9060;
    static final TableId PRODUCTS = TableId.of("shop", "products");

    private Emulator() {
    }

    static BigQueryEmulatorContainer container() {
        BigQueryEmulatorContainer container = new BigQueryEmulatorContainer(IMAGE);
        container.withCommand("--project", PROJECT_ID);
        container.withExposedPorts(9050, GRPC_PORT);
        return container;
    }

    static BigQueryConnection connection(BigQueryEmulatorContainer container) {
        return BigQueryConnection.emulator(
                PROJECT_ID,
                container.getEmulatorHttpEndpoint(),
                container.getHost() + ":" + container.getMappedPort(GRPC_PORT));
    }

    static void seed(BigQuery bigQuery) {
        if (Objects.nonNull(bigQuery.getDataset(PRODUCTS.getDataset()))) {
            return;
        }
        bigQuery.create(DatasetInfo.of(PRODUCTS.getDataset()));
        bigQuery.create(TableInfo.of(PRODUCTS, StandardTableDefinition.of(Schema.of(
                Field.of("id", StandardSQLTypeName.INT64),
                Field.of("name", StandardSQLTypeName.STRING),
                Field.of("price", StandardSQLTypeName.FLOAT64)))));
        InsertAllResponse response = bigQuery.insertAll(InsertAllRequest.newBuilder(PRODUCTS)
                .addRow(Map.of("id", 1, "name", "apple", "price", 1.2))
                .addRow(Map.of("id", 2, "name", "banana", "price", 3.5))
                .addRow(Map.of("id", 3, "name", "cherry", "price", 2.0))
                .build());
        if (response.hasErrors()) {
            throw new IllegalStateException(response.getInsertErrors().toString());
        }
    }
}
