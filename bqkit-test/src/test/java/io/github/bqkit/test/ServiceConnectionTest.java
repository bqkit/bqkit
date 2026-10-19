package io.github.bqkit.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import io.github.bqkit.core.QueryAvroReader;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
class ServiceConnectionTest {

    @Container
    @ServiceConnection
    static final BqkitBigQueryEmulatorContainer EMULATOR = new BqkitBigQueryEmulatorContainer();

    private static final TableId PRODUCTS = TableId.of("shop", "products");

    @Autowired
    BigQuery bigQuery;

    @Autowired
    QueryAvroReader reader;

    @Test
    void service_connection_wires_rest_and_grpc_clients_on_the_emulator() {
        seed();

        List<GenericRecord> rows = reader.query(
                "SELECT name FROM `" + EMULATOR.getProjectId() + ".shop.products` WHERE id = 2",
                TableId.of("shop", "query_result"));

        assertThat(rows).extracting(r -> r.get("name").toString()).containsExactly("banana");
    }

    private void seed() {
        bigQuery.create(DatasetInfo.of(PRODUCTS.getDataset()));
        bigQuery.create(TableInfo.of(PRODUCTS, StandardTableDefinition.of(Schema.of(
                Field.of("id", StandardSQLTypeName.INT64),
                Field.of("name", StandardSQLTypeName.STRING)))));
        bigQuery.insertAll(InsertAllRequest.newBuilder(PRODUCTS)
                .addRow(Map.of("id", 1, "name", "apple"))
                .addRow(Map.of("id", 2, "name", "banana"))
                .build());
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class TestApp {
    }
}
