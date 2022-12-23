package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.confluent.parallelconsumer.ManagedTruth.assertTruth;

/**
 * @author Antony Stubbs
 * @see ConsumerFacade
 */
@Slf4j
class ConsumerFacadeTest extends BrokerIntegrationTest<String, String> {

    Consumer<String, String> realConsumer = getKcu().getConsumer();

    ParallelEoSStreamProcessor<String, String> pc;

    PCConsumerAPI cf;

    TopicPartition tp;

    @BeforeEach
    void setup() {
        pc = getKcu().buildPc();
        cf = pc.consumerApiAccess().partialKafkaConsumer();

        setupTopic();

        var topic = getTopic();
        pc.subscribe(topic);
        pc.poll(recordContexts -> {
            log.debug("Got records: {}", recordContexts);
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
        assertTruth(assignment).containsExactly(new TopicPartition(getTopic(), 0));
        assertTruth(pc).isNotClosedOrFailed();
    }

//    @Test
//    void subscribe() {
//    }
//
//    @Test
//    void testSubscribe() {
//    }

    @Test
    void seek() {
        // facade test - move
        {
            Set<?> assignment = cf.assignment();
            assertTruth(assignment).isNotEmpty();
        }

        // wait for record consume

        // seek
        cf.seek(tp, 0L);

        // wait for record consume again
        {
            Set<?> assignment = cf.assignment();
            assertTruth(assignment).isNotEmpty();
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