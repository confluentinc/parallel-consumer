package io.confluent.csid.asyncconsumer;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

public class WorkManagerOffsetMapTest {

    WorkManager<String, String> wm;

    @BeforeEach
    void setup() {
        wm = new WorkManager<>(AsyncConsumerOptions.builder().build(), new MockConsumer<>(OffsetResetStrategy.EARLIEST));
    }

    @Test
    void serialisation() {
        String s = wm.makeOffsetMetadataPayload(Set.of(2L, 3L));
        assertThat(s).isEqualTo("");
    }

    @Test
    void deserialiseIncompleteOffsetMap() {
        TreeSet<Long> longs = wm.deserialiseIncompleteOffsetMap("incomplete offsets map");
        assertThat(longs).containsExactly(5L,8L);
    }

    @Test
    void serialiseIncompleteOffsetMap() {
        String s = wm.serialiseIncompleteOffsetMap(Set.of(1L, 5L, 8L));
        assertThat(s).isEqualTo("");
    }

}
