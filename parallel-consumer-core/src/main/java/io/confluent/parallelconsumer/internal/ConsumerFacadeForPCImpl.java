package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumer;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;

import static io.confluent.csid.utils.JavaUtils.isEmpty;
import static lombok.AccessLevel.PRIVATE;

/**
 * @author Antony Stubbs
 * @see ConsumerFacadeForPC
 */
@Slf4j
@ThreadSafe
@FieldDefaults(makeFinal = true, level = PRIVATE)
public class ConsumerFacadeForPCImpl<K, V> extends ConsumerFacadeStrictImpl<K, V> implements ConsumerFacadeForPC {

    ParallelConsumer<K, V> pcApi;

    public ConsumerFacadeForPCImpl(ParallelConsumer<K, V> pcApi, BrokerPollSystem<K, V> poller) {
        super(poller);
        this.pcApi = pcApi;
    }

    @Override
    public void pause(final Collection<TopicPartition> collection) {
        warnInvalidUse(collection);
        pcApi.pauseIfRunning();
    }

    private static void warnInvalidUse(Collection<?> collection) {
        if (!isEmpty(collection)) {
            log.warn("Pausing specific partitions not supported yet, pausing all");
        }
    }

    @Override
    public void resume(Collection<TopicPartition> collection) {
        warnInvalidUse(collection);
        pcApi.resumeIfPaused();
    }

    // option to have this shutdown PC?
    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close(final Duration timeout) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
