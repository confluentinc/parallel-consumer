package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Antony Stubbs
 * @see ConsumerFacadeForPCImpl
 */
@Slf4j
class ConsumerFacadeForPCImplTest extends BrokerIntegrationTest<String, String> {

    public static final List<TopicPartition> EMPTY_LIST = UniLists.of();

    ParallelEoSStreamProcessor<String, String> pc;

    ConsumerFacadeForPC cf;

    AtomicInteger consumed = new AtomicInteger();

    Queue<PollContext<String, String>> all = new ConcurrentLinkedQueue();

    @Test
    void resume() {
        cf.resume(EMPTY_LIST);
        throw new RuntimeException("Not implemented");
    }

    @Test
    void pause() {
        cf.pause(EMPTY_LIST);
        throw new RuntimeException("Not implemented");
    }


}