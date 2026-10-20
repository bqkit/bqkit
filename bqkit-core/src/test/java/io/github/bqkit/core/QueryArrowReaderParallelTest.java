package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.ArrowRecordBatch;
import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class QueryArrowReaderParallelTest {

    private static final Schema NAME = new Schema(List.of(Field.nullable("name", ArrowType.Utf8.INSTANCE)));

    private final BigQuery bigQuery = BigQueryClients.bigQuery(BigQueryConnection.google(
            "my-project", NoCredentials.getInstance()));
    private final BigQueryReadClient readClient = mock(BigQueryReadClient.class);
    private final ReadStreamsStub streams = new ReadStreamsStub(readClient);

    @Test
    void asks_for_as_many_streams_as_the_reader_reads_at_once() {
        session(0);

        new QueryArrowReader(bigQuery, readClient, 4).readTable(TableId.of("shop", "products"), batch -> { });

        ArgumentCaptor<CreateReadSessionRequest> request = ArgumentCaptor.forClass(CreateReadSessionRequest.class);
        verify(readClient).createReadSession(request.capture());
        assertThat(request.getValue().getMaxStreamCount()).isEqualTo(4);
    }

    @Test
    void rejects_reading_less_than_one_stream_at_once() {
        assertThatThrownBy(() -> new QueryArrowReader(bigQuery, readClient, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxStreams");
    }

    @Test
    void reads_the_streams_of_the_session_at_the_same_time_one_batch_at_a_time() {
        session(2);
        CountDownLatch bothOpen = new CountDownLatch(2);
        streams.stream("stream-0", () -> ReadStreamsStub.await(bothOpen), rows("apple", "banana"));
        streams.stream("stream-1", () -> ReadStreamsStub.await(bothOpen), rows("cherry"), rows("damson"));
        List<String> names = new ArrayList<>();
        AtomicInteger inside = new AtomicInteger();
        AtomicBoolean overlapped = new AtomicBoolean();
        CountDownLatch bothCalled = new CountDownLatch(2);

        new QueryArrowReader(bigQuery, readClient, 2).readTable(TableId.of("shop", "products"), batch -> {
            if (inside.incrementAndGet() > 1) {
                overlapped.set(true);
            }
            bothCalled.countDown();
            try {
                bothCalled.await(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            VarCharVector name = (VarCharVector) batch.getVector("name");
            for (int i = 0; i < batch.getRowCount(); i++) {
                names.add(name.getObject(i).toString());
            }
            inside.decrementAndGet();
        });

        assertThat(names).containsExactlyInAnyOrder("apple", "banana", "cherry", "damson");
        assertThat(overlapped).isFalse();
    }

    private void session(int streamCount) {
        ReadSession.Builder session = ReadSession.newBuilder()
                .setArrowSchema(ArrowSchema.newBuilder().setSerializedSchema(ArrowMessages.schema(NAME)));
        for (int i = 0; i < streamCount; i++) {
            session.addStreams(ReadStream.newBuilder().setName("stream-" + i));
        }
        when(readClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(session.build());
    }

    private static ReadRowsResponse rows(String... names) {
        return ReadRowsResponse.newBuilder()
                .setArrowRecordBatch(ArrowRecordBatch.newBuilder()
                        .setSerializedRecordBatch(ArrowMessages.recordBatch(NAME, names)))
                .build();
    }
}
