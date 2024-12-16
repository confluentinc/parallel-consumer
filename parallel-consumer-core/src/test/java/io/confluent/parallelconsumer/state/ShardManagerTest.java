package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

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
        String topic = "topic";
        int partition = 0;

        PCModule<String, String> mockPcModule = mock(PCModule.class);
        MutableClock clock = MutableClock.of(Instant.now(), Clock.systemDefaultZone().getZone());
        when(mockPcModule.clock()).thenReturn(clock);
        when(mockPcModule.options()).thenReturn(ParallelConsumerOptions.<String, String>builder().build());
        RetryQueue retryQueue = new RetryQueue();

        WorkContainer<String, String> w0 = new WorkContainer<>(0, new ConsumerRecord<>(topic, partition, 0, "k", "v"), mockPcModule);

        WorkContainer<String, String> w1 = new WorkContainer<>(0, new ConsumerRecord<>(topic, partition, 1, "k", "v"), mockPcModule);
        WorkContainer<String, String> w2 = new WorkContainer<>(0, new ConsumerRecord<>(topic, partition, 2, "k", "v"), mockPcModule);
        WorkContainer<String, String> w3 = new WorkContainer<>(0, new ConsumerRecord<>(topic, partition, 3, "k", "v"), mockPcModule);


        retryQueue.add(w0);
        retryQueue.add(w1);
        retryQueue.add(w2);
        retryQueue.add(w3);

        assertThat(retryQueue.size()).isEqualTo(4);

        assertThat(w0).isNotEqualTo(w1);
        assertThat(w1).isNotEqualTo(w2);

        boolean removed = retryQueue.remove(w1);
        assertThat(removed).isTrue();
        assertThat(retryQueue.size()).isEqualTo(3);

        Assertions.assertThat(checkForNoDupes(retryQueue)).as("RetryQueue should not contain duplicates").isTrue();

        assertThat(retryQueue.contains(w0)).isTrue();
        assertThat(retryQueue.contains(w1)).isFalse();

        assertThat(retryQueue.contains(w0)).isTrue();
        assertThat(retryQueue.contains(w1)).isFalse();
        assertThat(retryQueue.contains(w2)).isTrue();
    }

    @Test
    void testRetryQueueOrdering() {
        RetryQueue retryQueue = new RetryQueue();
        PCModule<String, String> mockPcModule = mock(PCModule.class);
        MutableClock clock = MutableClock.of(Instant.now(), Clock.systemDefaultZone().getZone());
        when(mockPcModule.clock()).thenReturn(clock);
        when(mockPcModule.options()).thenReturn(ParallelConsumerOptions.<String, String>builder().build());

        String topic = "topic";
        int partition = 0;

        WorkContainer<String, String> wc1 = new WorkContainer<>(0, new ConsumerRecord<>(topic, partition, 0, "k", "v"), mockPcModule);
        wc1.onUserFunctionFailure(new Throwable("cause"));
        retryQueue.add(wc1);
        clock.add(10, ChronoUnit.SECONDS);
        WorkContainer<String, String> wc1_2 = new WorkContainer<>(0, new ConsumerRecord<>(topic, partition, 0, "k", "v"), mockPcModule);
        wc1_2.onUserFunctionFailure(new Throwable("cause"));
        retryQueue.add(wc1_2);
        Assertions.assertThat(retryQueue.size()).isEqualTo(1);
    }

    @Test
    void testRetryQueueOrderingMultipleTries() {
        String topic = "topic";
        int partition = 0;
        int retryTestNum = 0;
        while (retryTestNum < 5) {

            PCModule<String, String> mockPcModule = mock(PCModule.class);
            MutableClock clock = MutableClock.of(Instant.now(), Clock.systemDefaultZone().getZone());
            when(mockPcModule.clock()).thenReturn(clock);
            when(mockPcModule.options()).thenReturn(ParallelConsumerOptions.<String, String>builder().build());

            RetryQueue retryQueue = new RetryQueue();


            WorkContainer<String, String> w0 = new WorkContainer<>(
                    1, new ConsumerRecord<>(topic, partition, 0, "key0", "value0"), mockPcModule);
            ((MutableClock) mockPcModule.clock()).setInstant(Instant.now());
            w0.onUserFunctionFailure(new RuntimeException("test1"));
            retryQueue.add(w0);

            WorkContainer<String, String> w1 = new WorkContainer<>(
                    1, new ConsumerRecord<>(topic, partition, 1, "key1", "value0"), mockPcModule);
            ThreadUtils.sleepQuietly(10);
            ((MutableClock) mockPcModule.clock()).setInstant(Instant.now());
            w1.onUserFunctionFailure(new RuntimeException("test2"));
            retryQueue.add(w1);

            WorkContainer<String, String> w2 = new WorkContainer<>(
                    1, new ConsumerRecord<>(topic, partition, 2, "key2", "value0"), mockPcModule);
            ThreadUtils.sleepQuietly(10);
            ((MutableClock) mockPcModule.clock()).setInstant(Instant.now());
            w2.onUserFunctionFailure(new RuntimeException("test3"));
            retryQueue.add(w2);

            ThreadUtils.sleepQuietly(10);
            ((MutableClock) mockPcModule.clock()).setInstant(Instant.now());
            w0.onUserFunctionFailure(new RuntimeException("a"));
            int tries = 0;
            while (retryQueue.size() < 4 && tries < 100) {
                ((MutableClock) mockPcModule.clock()).setInstant(Instant.now());
                w0.onUserFunctionFailure(new RuntimeException("a"));
                retryQueue.add(w0);
                tries++;
            }
            // Sometimes 4 elements are observed in retryQueue
            Assertions.assertThat(retryQueue.size()).as("Expecting to have 3 elements").isEqualTo(3);

            retryQueue.remove(w0);
            retryQueue.remove(w1);
            retryQueue.remove(w2);
            Assertions.assertThat(retryQueue.size()).isEqualTo(0);
            retryTestNum++;
        }
    }

    private boolean checkForNoDupes(RetryQueue retryQueue) {
        Set<String> checkSet = new HashSet<>();
        try (RetryQueue.RetryQueueIterator retryQueueIterator = retryQueue.iterator()) {
            while (retryQueueIterator.hasNext()) {
                WorkContainer<?, ?> workContainer = retryQueueIterator.next();
                //Checking by topic + partition + offset for uniqueness
                if (!checkSet.add(workContainer.getTopicPartition().topic() + "_" + workContainer.getTopicPartition().partition() + "_" + workContainer.getCr().offset())) {
                    return false;
                }
            }
        }
        return true;
    }
}