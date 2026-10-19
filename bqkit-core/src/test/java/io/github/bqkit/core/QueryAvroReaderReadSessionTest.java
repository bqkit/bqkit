package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class QueryAvroReaderReadSessionTest {

    private static final String EMPTY_RECORD = """
            {"type": "record", "name": "row", "fields": []}""";

    private final BigQuery bigQuery = BigQueryClients.bigQuery(new BigQueryConnection(
            "my-project", null, null, false, NoCredentials.getInstance()));
    private final BigQueryReadClient readClient = mock(BigQueryReadClient.class);

    @Test
    void reads_a_table_of_another_project_in_that_project() {
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(ReadSession.newBuilder()
                .setAvroSchema(AvroSchema.newBuilder().setSchema(EMPTY_RECORD))
                .build());

        new QueryAvroReader(bigQuery, readClient).readTable(TableId.of("other-project", "shop", "products"));

        ArgumentCaptor<CreateReadSessionRequest> request = ArgumentCaptor.forClass(CreateReadSessionRequest.class);
        verify(readClient).createReadSession(request.capture());
        assertThat(request.getValue().getParent()).isEqualTo("projects/other-project");
        assertThat(request.getValue().getReadSession().getTable())
                .isEqualTo("projects/other-project/datasets/shop/tables/products");
    }
}
