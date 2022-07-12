package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Antony Stubbs
 * @see ShardKey
 */
class ShardKeyTest {

    /**
     * Tests when KEY ordering with `null` keyed records
     */
    @Test
    void nullKey() {
        var cr = mock(ConsumerRecord.class);
        when(cr.partition()).thenReturn(0);
        when(cr.topic()).thenReturn("atopic");
        when(cr.key()).thenReturn(null);

        var wc = mock(WorkContainer.class);
        when(wc.getCr()).thenReturn(cr);

        ShardKey.of(wc, KEY);
    }

    // todo split up
    @Test
    void keyTest() {
        ParallelConsumerOptions.ProcessingOrder ordering = KEY;
        String topicOne = "t1";
        TopicPartition topicOneP0 = new TopicPartition("t1", 0);
        String keyOne = "k1";


        // same inputs, different key instances equal
        var reck1 = new ConsumerRecord<>(topicOne, 0, 0, keyOne, "v");
        ShardKey key1 = ShardKey.of(reck1, ordering);
        ShardKey anotherInstanceWithSameInputs = ShardKey.of(reck1, ordering);
        assertThat(key1).isEqualTo(anotherInstanceWithSameInputs);

        // same topic, same partition, different key
        var reck2 = new ConsumerRecord<>(topicOne, 0, 0, "k2", "v");
        ShardKey of3 = ShardKey.of(reck2, ordering);
        assertThat(key1).isNotEqualTo(of3);

        // different topic, same key
        var reck3 = new ConsumerRecord<>("t2", 0, 0, keyOne, "v");
        assertThat(key1).isNotEqualTo(ShardKey.of(reck3, ordering));

        // same topic, same key
        ShardKey.KeyOrderedKey keyOrderedKey = new ShardKey.KeyOrderedKey(topicOneP0, keyOne);
        ShardKey.KeyOrderedKey keyOrderedKeyTwo = new ShardKey.KeyOrderedKey(topicOneP0, keyOne);
        assertThat(keyOrderedKey).isEqualTo(keyOrderedKeyTwo);

        // same topic, same key, different partition
        var reck4 = new ConsumerRecord<>(topicOne, 1, 0, keyOne, "v");
        ShardKey of4 = ShardKey.of(reck2, ordering);
        assertThat(key1).isNotEqualTo(of3);

        // check both exist in queue too ??
        //assertThat("false").isEmpty();
    }

}
