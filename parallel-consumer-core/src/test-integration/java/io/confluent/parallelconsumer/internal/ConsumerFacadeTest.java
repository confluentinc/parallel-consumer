package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.confluent.parallelconsumer.ManagedTruth.assertTruth;

/**
 * @author Antony Stubbs
 * @see ConsumerFacade
 */
class ConsumerFacadeTest extends BrokerIntegrationTest {

    Consumer<String, String> realConsumer = super.kcu.getConsumer();
    AbstractParallelEoSStreamProcessor<String, String> pc;

    ConsumerFacade<String, String> cf;

    TopicPartition tp;

    @Test
    void assignment() {
        Set<?> assignment = cf.assignment();
        assertTruth(assignment).isNotEmpty();
    }

    @Test
    void subscribe() {
    }

    @Test
    void testSubscribe() {
    }

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

    @Test
    void position() {
    }

    @Test
    void metrics() {
    }

    @Test
    void listTopics() {
    }

    @Test
    void currentLag() {
    }

    @Test
    void endOffsets() {
    }

    @Test
    void beginningOffsets() {
    }

    @Test
    void offsetsForTimes() {
    }

    @Test
    void committed() {
    }

    @Test
    void seekToEnd() {
    }

    @Test
    void seekToBeginning() {
    }

    @Test
    void assign() {
    }

    @Test
    void testSubscribe1() {
    }

    @Test
    void testSubscribe2() {
    }

    @Test
    void poll() {
    }

    @Test
    void testPoll() {
    }

    @Test
    void commitSync() {
    }

    @Test
    void resume() {
    }

    @Test
    void pause() {
    }

    @Test
    void wakeup() {
    }
}