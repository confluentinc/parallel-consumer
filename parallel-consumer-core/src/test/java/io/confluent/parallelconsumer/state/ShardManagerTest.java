package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.google.common.truth.Truth.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * @author Antony Stubbs
 * @see ShardManager
 */
class ShardManagerTest {

    ModelUtils mu = new ModelUtils();
    PartitionState<String, String> state;
    WorkManager<String, String> wm;

    String topic = "myTopic";
    int partition = 0;

    TopicPartition tp = new TopicPartition(topic, partition);

    ConcurrentSkipListMap<Long, Optional<ConsumerRecord<String, String>>> incompleteOffsets = new ConcurrentSkipListMap<>();

    @BeforeEach
    void setup() {
        state = new PartitionState<>(0, mu.getModule(), tp, OffsetMapCodecManager.HighestOffsetAndIncompletes.of());
        wm = mu.getModule().workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
    }

    @Test
    void testAssignedQuickRevokeNPE() {
        // issue : https://github.com/confluentinc/parallel-consumer/issues/757
        // 1. partition assigned and incompleteOffsets existed
        // 2. right before begin to poll and process messages, it got revoked
        // 3. the processingShard has no data yet
        // 4. when revoked, try to delete entries with records from incompleteOffsets, no such record in entries
        PCModuleTestEnv module = mu.getModule();
        ShardManager<String, String> sm = new ShardManager<>(module, module.workManager());
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(topic, partition, 1, null, "test1");

        Map<ShardKey, ProcessingShard<String, String>> processingShards = new ConcurrentHashMap<>();
        processingShards.put(ShardKey.ofKey(consumerRecord), new ProcessingShard<>(ShardKey.ofKey(consumerRecord), module.options(), wm.getPm()));
        sm.setProcessingShards(processingShards);
        incompleteOffsets.put(1L, Optional.of(consumerRecord));
        state.setIncompleteOffsets(incompleteOffsets);
        state.onPartitionsRemoved(sm);
        assertThat(sm.getShard(ShardKey.ofKey(consumerRecord))).isEmpty();
    }

    @Test
    void retryQueueOrdering() {
        PCModuleTestEnv module = mu.getModule();
        ShardManager<String, String> sm = new ShardManager<>(module, module.workManager());
        NavigableSet<WorkContainer<?, ?>> retryQueue = sm.getRetryQueue();


        WorkContainer<String, String> w0 = mu.createWorkFor(0);
        WorkContainer<String, String> w1 = mu.createWorkFor(1);
        WorkContainer<String, String> w2 = mu.createWorkFor(2);
        WorkContainer<String, String> w3 = mu.createWorkFor(3);

        final int ZERO = 0;
        assertThat(sm.getRetryQueueWorkContainerComparator().compare(w0, w0)).isEqualTo(ZERO);


        retryQueue.add(w0);
        retryQueue.add(w1);
        retryQueue.add(w2);
        retryQueue.add(w3);

        assertThat(retryQueue).hasSize(4);

        assertThat(w0).isNotEqualTo(w1);
        assertThat(w1).isNotEqualTo(w2);

        boolean removed = retryQueue.remove(w1);
        assertThat(removed).isTrue();
        assertThat(retryQueue).hasSize(3);

        assertThat(retryQueue).containsNoDuplicates();

        assertThat(retryQueue.contains(w0)).isTrue();
        assertThat(retryQueue.contains(w1)).isFalse();

        assertThat(retryQueue).contains(w0);
        assertThat(retryQueue).containsNoneIn(of(w1));
        assertThat(retryQueue).contains(w2);
    }
}