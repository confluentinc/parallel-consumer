
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests;

import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.state.ShardKey;
import io.confluent.parallelconsumer.state.ShardKey.KeyOrderedKey;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static one.util.streamex.StreamEx.of;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

/**
 * Originally created to investigate issue report #184
 */
@Slf4j
class MultiTopicTest extends BrokerIntegrationTest<String, String> {


    @ParameterizedTest
    @EnumSource(ProcessingOrder.class)
    void multiTopic(ProcessingOrder order) {
        int numTopics = 3;
        List<NewTopic> multiTopics = getKcu().createTopics(numTopics);
        int recordsPerTopic = 1;
        multiTopics.forEach(singleTopic -> sendMessages(singleTopic, recordsPerTopic));

        var pc = getKcu().buildPc(order);
        pc.subscribe(of(multiTopics).map(NewTopic::name).toList());

        AtomicInteger messageProcessedCount = new AtomicInteger();

        pc.poll(pollContext -> {
            log.debug(pollContext.toString());
            messageProcessedCount.incrementAndGet();
        });

        // processed
        int expectedMessagesCount = recordsPerTopic * numTopics;
        await().untilAtomic(messageProcessedCount, Matchers.is(equalTo(expectedMessagesCount)));

        // commits
//        await().untilAsserted(() -> {
//            multiTopics.forEach(singleTopic -> assertCommit(singleTopic, recordsPerTopic));
//        });
    }


    @SneakyThrows
    private void sendMessages(NewTopic newTopic, int recordsPerTopic) {
        getKcu().produceMessages(newTopic.name(), recordsPerTopic);
    }

    private void assertCommit(NewTopic newTopic, int recordsPerTopic) {
        assertThat(getKcu().getLastConsumerConstructed())
                .hasCommittedToPartition(newTopic)
                .offset(recordsPerTopic);
    }

    // todo split out
    @Test
    void keyTest() {
        ProcessingOrder ordering = KEY;
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
        KeyOrderedKey keyOrderedKey = new KeyOrderedKey(topicOne, keyOne);
        KeyOrderedKey keyOrderedKeyTwo = new KeyOrderedKey(topicOne, keyOne);
        assertThat(keyOrderedKey).isEqualTo(keyOrderedKeyTwo);

        // same topic, same key, different partition
        var reck4 = new ConsumerRecord<>(topicOne, 1, 0, keyOne, "v");
        ShardKey of4 = ShardKey.of(reck2, ordering);
        assertThat(key1).isNotEqualTo(of3);
        // check both exist in queue too
        assertThat("false").isEmpty();
    }

}
