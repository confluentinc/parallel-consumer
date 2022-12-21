package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.ConsumerApiAccess;
import lombok.Value;

@Value
public class ConsumerAccessImpl<K, V> implements ConsumerApiAccess {

    BrokerPollSystem<K, V> basePollerRef;

    @Override
    public FullConsumerFacade<K, V> fullConsumerFacade() {
        // new or reused?
        return new FullConsumerFacade<>(basePollerRef);
    }

    @Override
    public PCConsumerAPI partialKafkaConsumer() {
        // new or reused?
        return new ConsumerFacade<>(basePollerRef);
    }
}
