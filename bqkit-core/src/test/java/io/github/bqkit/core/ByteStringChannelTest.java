package io.github.bqkit.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import org.junit.jupiter.api.Test;

class ByteStringChannelTest {

    @Test
    void hands_out_every_byte_through_a_destination_smaller_than_the_payload() throws IOException {
        ByteString payload = ByteString.copyFromUtf8("0123456789");
        ByteStringChannel channel = new ByteStringChannel(payload);
        ByteBuffer destination = ByteBuffer.allocate(4);
        ByteArrayOutputStream read = new ByteArrayOutputStream();
        int count;

        while ((count = channel.read(destination.clear())) != -1) {
            assertThat(count).isEqualTo(destination.position());
            read.write(destination.array(), 0, count);
        }

        assertThat(read.toString()).isEqualTo("0123456789");
        assertThat(payload.toStringUtf8()).isEqualTo("0123456789");
    }

    @Test
    void reads_at_most_what_the_destination_can_take() throws IOException {
        ByteStringChannel channel = new ByteStringChannel(ByteString.copyFromUtf8("abc"));
        ByteBuffer destination = ByteBuffer.allocate(8);

        assertThat(channel.read(destination)).isEqualTo(3);
        assertThat(channel.read(destination)).isEqualTo(-1);
    }

    @Test
    void reads_nothing_into_a_full_destination() throws IOException {
        ByteStringChannel channel = new ByteStringChannel(ByteString.copyFromUtf8("abc"));

        assertThat(channel.read(ByteBuffer.allocate(0))).isZero();
        assertThat(channel.read(ByteBuffer.allocate(3))).isEqualTo(3);
    }

    @Test
    void reports_the_end_of_the_payload_even_to_a_full_destination() throws IOException {
        ByteStringChannel channel = new ByteStringChannel(ByteString.EMPTY);

        assertThat(channel.read(ByteBuffer.allocate(0))).isEqualTo(-1);
    }

    @Test
    void hands_out_every_byte_of_a_payload_made_of_several_chunks() throws IOException {
        String first = "a".repeat(200);
        String second = "b".repeat(200);
        ByteString payload = ByteString.copyFromUtf8(first).concat(ByteString.copyFromUtf8(second));
        assertThat(payload.asReadOnlyByteBufferList()).hasSize(2);
        ByteStringChannel channel = new ByteStringChannel(payload);
        ByteBuffer destination = ByteBuffer.allocate(150);
        ByteArrayOutputStream read = new ByteArrayOutputStream();
        int count;

        while ((count = channel.read(destination.clear())) != -1) {
            read.write(destination.array(), 0, count);
        }

        assertThat(read.toString()).isEqualTo(first + second);
    }

    @Test
    void stops_reading_once_closed() {
        ByteStringChannel channel = new ByteStringChannel(ByteString.copyFromUtf8("abc"));
        assertThat(channel.isOpen()).isTrue();

        channel.close();

        assertThat(channel.isOpen()).isFalse();
        assertThatThrownBy(() -> channel.read(ByteBuffer.allocate(3))).isInstanceOf(ClosedChannelException.class);
    }
}
