package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import com.google.common.truth.Truth8;
import io.confluent.parallelconsumer.ManagedTruth;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.ManagedTruth.assertTruth;
import static org.awaitility.Awaitility.await;

/**
 * @author Antony Stubbs
 * @see ConsumerFacadeStrictImpl
 */
@Slf4j
class PartialConsumerFacadeStrictImplTest extends BrokerIntegrationTest<String, String> {

    ParallelEoSStreamProcessor<String, String> pc;

    PCConsumerAPIStrict cf;

    AtomicInteger consumed = new AtomicInteger();

    Queue<PollContext<String, String>> all = new ConcurrentLinkedQueue();


    @BeforeEach
    void setup() {
        pc = getKcu().buildPc();
        cf = pc.consumerApiAccess().partialStrictKafkaConsumer();

        setupTopic();

        var topic = getTopic();
        pc.subscribe(topic);
        pc.poll(recordContexts -> {
            log.debug("Got records: {}", recordContexts);
            consumed.incrementAndGet();
            all.add(recordContexts);
        });

    }

    @AfterEach
    void tearDown() {
        pc.close();
    }

    @SneakyThrows
    @Test
    void assignment() {
        var assignment = cf.assignment();
        assertTruth(assignment).isNotEmpty();
        assertTruth(assignment).containsExactly(getTopicPartition());
        assertTruth(pc).isNotClosedOrFailed();
    }

    @NotNull
    private TopicPartition getTopicPartition() {
        return new TopicPartition(getTopic(), 0);
    }

    @Test
    void subscribe() {
        cf.subscribe(getTopicPartitionList());
        throw new RuntimeException("Not implemented");
    }

    private List<TopicPartition> getTopicPartitionList() {
        return UniLists.of(getTopicPartition());
    }

    @SneakyThrows
    @Test
    void seek() {
        var other = 2;
        getKcu().produceMessages(getTopic(), other);

        // facade test - move
        {
            Set<?> assignment = cf.assignment();
            assertTruth(assignment).isNotEmpty();
        }

        // wait for record consume
        await().untilAsserted(() -> Truth.assertThat(consumed.get()).isAtLeast(other));

        // seek
        cf.seek(getTopicPartition(), 0L);

        // wait for record consume
        await().untilAsserted(() -> Truth.assertThat(consumed.get()).isAtLeast(other * 2));

        // wait for record consume again
        {
            Set<?> assignment = cf.assignment();
            assertTruth(assignment).isNotEmpty();
        }

        {
            var collect = all.stream().collect(Collectors.toList());
            assertTruth(collect).hasSize(4);
            assertTruth(collect).hasSize(4);
        }
    }

    @Test
    void position() {
        var position = cf.position(getTopicPartition());
        Truth.assertThat(position).isEqualTo(42L);
    }

    @Test
    void metrics() {
        Map<MetricName, ? extends Metric> metrics = cf.metrics();
        assertTruth(metrics).isNotEmpty();
        Truth.assertThat(metrics.size()).isGreaterThan(100);
    }

    @Test
    void listTopics() {
        var stringListMap = cf.listTopics();
        assertTruth(stringListMap).isNotEmpty();
        Truth.assertThat(stringListMap.size()).isGreaterThan(2);
        assertTruth(stringListMap.keySet()).contains(getTopic());
    }

    @Test
    void currentLag() {
        var lag = cf.currentLag(getTopicPartition());
        Truth8.assertThat(lag).isPresent();
        Truth.assertThat(lag.getAsLong()).isEqualTo(0L);
    }

    @Test
    void endOffsets() {
        var endOffsets = cf.endOffsets(getTopicPartitionList());
        Truth.assertThat(endOffsets.entrySet().iterator().next().getValue()).isEqualTo(4);
    }

    @Test
    void beginningOffsets() {
        var beginningOffsets = cf.beginningOffsets(getTopicPartitionList());
        Truth.assertThat(beginningOffsets).hasSize(1);
        Truth.assertThat(beginningOffsets.entrySet().iterator().next().getValue()).isEqualTo(0);
    }

    @Test
    void offsetsForTimes() {
        var lag = cf.offsetsForTimes(UniMaps.of(getTopicPartition(), 0L));
        Truth.assertThat(lag).hasSize(1);
        var offsetAndTimestamp = lag.get(getTopicPartition());
        Truth.assertThat(offsetAndTimestamp.offset()).isEqualTo(-4);
    }

    @Test
    void committed() {
        var lag = cf.committed(getTopicPartition());
        ManagedTruth.assertThat(lag).isNull();
    }

    @Test
    void seekToEnd() {
        cf.seekToEnd(getTopicPartitionList());
        throw new RuntimeException("Not implemented");
    }

    @Test
    void seekToBeginning() {
        cf.seekToEnd(getTopicPartitionList());
        throw new RuntimeException("Not implemented");
    }

    @Test
    void assign() {
        cf.assign(getTopicPartitionList());
        throw new RuntimeException("Not implemented");
    }

    @Test
    void commitSync() {
        cf.commitSync();
        throw new RuntimeException("Not implemented");
    }

    @Test
    void wakeup() {
        throw new RuntimeException("Not implemented");
    }

}
