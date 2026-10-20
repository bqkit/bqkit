package io.github.bqkit.core;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The streams of a read session, each answering its own read: a stream runs {@code onOpen} on the thread reading
 * it before its first response, so that a test can wait for several streams to be read at the same time.
 */
final class ReadStreamsStub {

    private static final long TIMEOUT_SECONDS = 5;

    private final ServerStreamingCallable<ReadRowsRequest, ReadRowsResponse> readRows;

    @SuppressWarnings("unchecked")
    ReadStreamsStub(BigQueryReadClient readClient) {
        readRows = mock(ServerStreamingCallable.class);
        when(readClient.readRowsCallable()).thenReturn(readRows);
    }

    @SuppressWarnings("unchecked")
    ServerStream<ReadRowsResponse> stream(String name, Runnable onOpen, ReadRowsResponse... responses) {
        ServerStream<ReadRowsResponse> stream = mock(ServerStream.class);
        doReturn(stream).when(readRows).call(argThat(request -> request.getReadStream().equals(name)));
        when(stream.iterator()).thenAnswer(invocation -> {
            onOpen.run();
            return List.of(responses).iterator();
        });
        return stream;
    }

    /** A stream whose opening returns only once {@code latch} is counted down. */
    @SuppressWarnings("unchecked")
    ServerStream<ReadRowsResponse> streamOpenedAfter(String name, CountDownLatch latch) {
        ServerStream<ReadRowsResponse> stream = mock(ServerStream.class);
        doAnswer(invocation -> {
            waitFor(latch);
            return stream;
        }).when(readRows).call(argThat(request -> request.getReadStream().equals(name)));
        return stream;
    }

    /** A stream that sends nothing until it is cancelled, then ends. */
    @SuppressWarnings("unchecked")
    ServerStream<ReadRowsResponse> streamUntilCancelled(String name, Runnable onOpen) {
        ServerStream<ReadRowsResponse> stream = mock(ServerStream.class);
        CountDownLatch cancelled = new CountDownLatch(1);
        doReturn(stream).when(readRows).call(argThat(request -> request.getReadStream().equals(name)));
        doAnswer(invocation -> {
            cancelled.countDown();
            return null;
        }).when(stream).cancel();
        when(stream.iterator()).thenAnswer(invocation -> {
            onOpen.run();
            return new Iterator<ReadRowsResponse>() {
                @Override
                public boolean hasNext() {
                    waitFor(cancelled);
                    return false;
                }

                @Override
                public ReadRowsResponse next() {
                    throw new IllegalStateException("no response");
                }
            };
        });
        return stream;
    }

    /** Counts down then waits for the other streams: fails when the streams are not read at the same time. */
    static void await(CountDownLatch latch) {
        latch.countDown();
        waitFor(latch);
    }

    static void waitFor(CountDownLatch latch) {
        try {
            if (!latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out: the streams are not read at the same time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
