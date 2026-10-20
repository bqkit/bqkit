package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ReadStreamsTest {

    private static final List<ReadStream> TWO_STREAMS = List.of(
            ReadStream.newBuilder().setName("stream-0").build(),
            ReadStream.newBuilder().setName("stream-1").build());

    private final BigQueryReadClient readClient = mock(BigQueryReadClient.class);
    private final ReadStreamsStub streams = new ReadStreamsStub(readClient);

    @Test
    void throws_the_io_failure_of_a_stream_as_is() {
        streams.stream("stream-0", () -> { });
        streams.stream("stream-1", () -> { });
        IOException failure = new IOException("unreadable");

        assertThatThrownBy(() -> ReadStreams.<String>read(readClient, TWO_STREAMS, value -> { },
                (responses, consumer) -> {
                    throw failure;
                }))
                .isSameAs(failure);
    }

    @Test
    void throws_the_error_of_a_stream_as_is() {
        streams.stream("stream-0", () -> { });
        streams.stream("stream-1", () -> { });
        AssertionError failure = new AssertionError("broken");

        assertThatThrownBy(() -> ReadStreams.<String>read(readClient, TWO_STREAMS, value -> { },
                (responses, consumer) -> {
                    throw failure;
                }))
                .isSameAs(failure);
    }

    @Test
    void does_not_read_a_stream_opened_after_the_read_stopped() throws IOException {
        CountDownLatch stopped = new CountDownLatch(1);
        ServerStream<ReadRowsResponse> failing = streams.stream("stream-0", () -> { });
        doAnswer(invocation -> {
            stopped.countDown();
            return null;
        }).when(failing).cancel();
        ServerStream<ReadRowsResponse> late = streams.streamOpenedAfter("stream-1", stopped);
        IllegalStateException failure = new IllegalStateException("stream-0 failed");

        assertThatThrownBy(() -> ReadStreams.<String>read(readClient, TWO_STREAMS, value -> { },
                (responses, consumer) -> {
                    if (responses == failing) {
                        throw failure;
                    }
                    responses.iterator();
                }))
                .isSameAs(failure);

        verify(late).cancel();
        verify(late, never()).iterator();
    }

    @Test
    void stops_the_streams_when_the_reading_thread_is_interrupted() throws InterruptedException {
        CountDownLatch bothOpen = new CountDownLatch(2);
        ServerStream<ReadRowsResponse> first = streams.streamUntilCancelled("stream-0", bothOpen::countDown);
        ServerStream<ReadRowsResponse> second = streams.streamUntilCancelled("stream-1", bothOpen::countDown);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread reading = Thread.ofVirtual().start(() -> {
            try {
                ReadStreams.<ReadRowsResponse>read(readClient, TWO_STREAMS, value -> { },
                        (responses, consumer) -> {
                            for (ReadRowsResponse response : responses) {
                                consumer.accept(response);
                            }
                        });
            } catch (IOException | RuntimeException | Error e) {
                thrown.set(e);
            }
            interrupted.set(Thread.currentThread().isInterrupted());
        });
        ReadStreamsStub.waitFor(bothOpen);

        reading.interrupt();
        reading.join();

        assertThat(thrown.get()).isNotNull();
        assertThat(interrupted).isTrue();
        verify(first, atLeastOnce()).cancel();
        verify(second, atLeastOnce()).cancel();
    }
}
