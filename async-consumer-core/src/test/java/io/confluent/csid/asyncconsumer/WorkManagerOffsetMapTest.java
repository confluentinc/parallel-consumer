package io.confluent.csid.asyncconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniSets;

import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

public class WorkManagerOffsetMapTest {

    WorkManager<String, String> wm;

    TopicPartition tp = new TopicPartition("myTopic", 0);

    Set<Long> incomplete = new TreeSet<>(UniSets.of(2L, 3L));

    String expected = "rO0ABXNyABFqYXZhLnV0aWwuVHJlZVNldN2YUJOV7YdbAwAAeHBwdwQAAAACc3IADmphdmEubGFuZy5Mb25nO4vkkMyPI98CAAFKAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAAAAAAACc3EAfgACAAAAAAAAAAN4";

    @BeforeEach
    void setup() {
        wm = new WorkManager<>(AsyncConsumerOptions.builder().build(), new MockConsumer<>(OffsetResetStrategy.EARLIEST));
        wm.partitionOffsetHighWaterMarks.put(tp, 4L);
    }

    @Test
    void serialisationOfPayload() {
        String s = wm.makeOffsetMetadataPayload(tp, incomplete);
        assertThat(s).isEqualTo("4," + expected);
    }

    @Test
    void serialiseIncompleteOffsetMap() {
        String s = wm.serialiseIncompleteOffsetMap(incomplete);
        assertThat(s).isEqualTo(expected);
    }

    @Test
    void deserialiseIncompleteOffsetMap() {
        Set<Long> longs = wm.deserialiseIncompleteOffsetMap(expected);
        assertThat(longs.toArray()).containsExactly(incomplete.toArray());
    }

}
