package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ConsumerApiAccess;
import io.confluent.parallelconsumer.ParallelConsumer;
import lombok.Value;

/**
 * @author Antony Stubbs
 * @see ConsumerApiAccess
 */
@Value
public class ConsumerAccessImpl<K, V> implements ConsumerApiAccess<K, V> {

    BrokerPollSystem<K, V> poller;

    ParallelConsumer<K, V> pcApi;

    @Override
    public FullConsumerFacadeImpl<K, V> fullConsumerFacade() {
        // new or reused?
        return new FullConsumerFacadeImpl<>(pcApi, poller);
    }

    @Override
    public ConsumerFacadeForPC consumerFacadeForPC() {
        return new ConsumerFacadeForPCImpl<>(pcApi, poller);
    }

    @Override
    public PCConsumerAPIStrict partialStrictKafkaConsumer() {
        // new or reused?
        return new ConsumerFacadeStrictImpl<>(poller);
    }
}
