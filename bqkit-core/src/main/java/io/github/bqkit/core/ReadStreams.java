package io.github.bqkit.core;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Reads the streams of a read session, each on its own virtual thread when the session has several.
 *
 * <p>The consumer is called by one stream at a time, so it does not have to be thread-safe, and a stream waiting
 * for the consumer does not request its next response. The first failure stops the read: the other streams are
 * cancelled and that failure is thrown.
 */
final class ReadStreams<T> {

    /** Reads the responses of one stream into the consumer. */
    @FunctionalInterface
    interface StreamRead<T> {
        void read(Iterable<ReadRowsResponse> responses, Consumer<T> consumer) throws IOException;
    }

    private final BigQueryReadClient readClient;
    private final StreamRead<T> read;
    private final Consumer<T> consumer;
    private final Lock delivery = new ReentrantLock();
    private final Queue<ServerStream<ReadRowsResponse>> opened = new ConcurrentLinkedQueue<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean stopped;

    private ReadStreams(BigQueryReadClient readClient, StreamRead<T> read, Consumer<T> consumer) {
        this.readClient = readClient;
        this.read = read;
        this.consumer = consumer;
    }

    static <T> void read(BigQueryReadClient readClient, List<ReadStream> streams, Consumer<T> consumer,
                         StreamRead<T> read) throws IOException {
        if (streams.size() > 1) {
            new ReadStreams<>(readClient, read, consumer).readAtOnce(streams);
            return;
        }
        for (ReadStream stream : streams) {
            ServerStream<ReadRowsResponse> responses = readClient.readRowsCallable().call(request(stream));
            try {
                read.read(responses, consumer);
            } catch (IOException | RuntimeException | Error e) {
                ReadSessions.cancel(responses, e);
                throw e;
            }
            responses.cancel();
        }
    }

    private void readAtOnce(List<ReadStream> streams) throws IOException {
        ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor();
        try {
            streams.forEach(stream -> threads.execute(() -> readStream(stream)));
            threads.shutdown();
            threads.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            stop(new IllegalStateException("Interrupted while reading the table", e));
            Thread.currentThread().interrupt();
        } finally {
            threads.close();
        }
        rethrow(failure.get());
    }

    private void readStream(ReadStream stream) {
        try {
            ServerStream<ReadRowsResponse> responses = readClient.readRowsCallable().call(request(stream));
            opened.add(responses);
            if (stopped) {
                responses.cancel();
                return;
            }
            read.read(responses, this::deliver);
            responses.cancel();
        } catch (IOException | RuntimeException | Error e) {
            stop(e);
        }
    }

    private void deliver(T value) {
        delivery.lock();
        try {
            if (stopped) {
                throw new CancellationException("The read stopped on the failure of another stream");
            }
            consumer.accept(value);
        } finally {
            delivery.unlock();
        }
    }

    /** Keeps the first failure only: the later ones are the streams cancelled because of it. */
    private void stop(Throwable cause) {
        if (!failure.compareAndSet(null, cause)) {
            return;
        }
        stopped = true;
        opened.forEach(responses -> ReadSessions.cancel(responses, cause));
    }

    private static void rethrow(Throwable failure) throws IOException {
        if (failure instanceof IOException e) {
            throw e;
        }
        if (failure instanceof RuntimeException e) {
            throw e;
        }
        if (failure instanceof Error e) {
            throw e;
        }
    }

    private static ReadRowsRequest request(ReadStream stream) {
        return ReadRowsRequest.newBuilder().setReadStream(stream.getName()).build();
    }
}
