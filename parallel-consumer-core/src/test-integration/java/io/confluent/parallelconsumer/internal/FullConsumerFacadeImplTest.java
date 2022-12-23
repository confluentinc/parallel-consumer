package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Antony Stubbs
 * @see FullConsumerFacadeImpl
 */
@Slf4j
class FullConsumerFacadeImplTest extends BrokerIntegrationTest<String, String> {

    ParallelEoSStreamProcessor<String, String> pc;

    PCConsumerAPIStrict cf;

    AtomicInteger consumed = new AtomicInteger();

    Queue<PollContext<String, String>> all = new ConcurrentLinkedQueue();

//    TopicPartition tp;

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

    @Test
    void poll() {
        throw new RuntimeException("Not implemented");
    }

}