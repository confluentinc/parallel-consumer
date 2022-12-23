package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.ManagedTruth.assertTruth;
import static org.awaitility.Awaitility.await;

/**
 * @author Antony Stubbs
 * @see ConsumerFacade
 */
@Slf4j
class ConsumerFacadeTest extends BrokerIntegrationTest<String, String> {

    Consumer<String, String> realConsumer = getKcu().getConsumer();

    ParallelEoSStreamProcessor<String, String> pc;

    PCConsumerAPI cf;

    AtomicInteger consumed = new AtomicInteger();

    Queue<PollContext<String, String>> all = new ConcurrentLinkedQueue();

//    TopicPartition tp;

    @BeforeEach
    void setup() {
        pc = getKcu().buildPc();
        cf = pc.consumerApiAccess().partialKafkaConsumer();

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

//    @Test
//    void subscribe() {
//    }
//
//    @Test
//    void testSubscribe() {
//    }

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

//    @Test
//    void position() {
//    }
//
//    @Test
//    void metrics() {
//    }
//
//    @Test
//    void listTopics() {
//    }
//
//    @Test
//    void currentLag() {
//    }
//
//    @Test
//    void endOffsets() {
//    }
//
//    @Test
//    void beginningOffsets() {
//    }
//
//    @Test
//    void offsetsForTimes() {
//    }
//
//    @Test
//    void committed() {
//    }
//
//    @Test
//    void seekToEnd() {
//    }
//
//    @Test
//    void seekToBeginning() {
//    }
//
//    @Test
//    void assign() {
//    }
//
//    @Test
//    void testSubscribe1() {
//    }
//
//    @Test
//    void testSubscribe2() {
//    }
//
//    @Test
//    void poll() {
//    }
//
//    @Test
//    void testPoll() {
//    }
//
//    @Test
//    void commitSync() {
//    }
//
//    @Test
//    void resume() {
//    }
//
//    @Test
//    void pause() {
//    }
//
//    @Test
//    void wakeup() {
//    }
}