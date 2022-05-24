package io.confluent.parallelconsumer.state;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;

class ShardKeyTest {

    @Test
    void keyTest() {
        ParallelConsumerOptions.ProcessingOrder ordering = KEY;
        String topicOne = "t1";
        String keyOne = "k1";

        var reck1 = new ConsumerRecord<>(topicOne, 0, 0, keyOne, "v");
        ShardKey key1 = ShardKey.of(reck1, ordering);
        assertThat(key1).isEqualTo(ShardKey.of(reck1, ordering));

        // same topic, same partition, different key
        var reck2 = new ConsumerRecord<>(topicOne, 0, 0, "k2", "v");
        ShardKey of3 = ShardKey.of(reck2, ordering);
        assertThat(key1).isNotEqualTo(of3);

        // different topic, same key
        var reck3 = new ConsumerRecord<>("t2", 0, 0, keyOne, "v");
        assertThat(key1).isNotEqualTo(ShardKey.of(reck3, ordering));

        // same topic, same key
        ShardKey.KeyOrderedKey keyOrderedKey = new ShardKey.KeyOrderedKey(topicOne, keyOne);
        ShardKey.KeyOrderedKey keyOrderedKeyTwo = new ShardKey.KeyOrderedKey(topicOne, keyOne);
        assertThat(keyOrderedKey).isEqualTo(keyOrderedKeyTwo);

        // same topic, same key, different partition
        var reck4 = new ConsumerRecord<>(topicOne, 1, 0, keyOne, "v");
        ShardKey of4 = ShardKey.of(reck2, ordering);
        assertThat(key1).isNotEqualTo(of3);
        // check both exist in queue too
        assertThat("false").isEmpty();
    }

}
