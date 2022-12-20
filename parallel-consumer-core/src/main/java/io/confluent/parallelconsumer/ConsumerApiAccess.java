package io.confluent.parallelconsumer;

import io.confluent.parallelconsumer.internal.FullConsumerFacade;
import io.confluent.parallelconsumer.internal.PCConsumerAPI;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * @author Antony Stubbs
 * @see ConsumerFacade
 */
public interface ConsumerApiAccess<K, V> {

    /**
     * @return todo docs
     * @see FullConsumerFacade
     */
    FullConsumerFacade<K, V> fullConsumerFacade();

    /**
     * Partial implementation of {@link Consumer}
     *
     * @see PCConsumerAPI
     */
    PCConsumerAPI partialKafkaConsumer();
}
