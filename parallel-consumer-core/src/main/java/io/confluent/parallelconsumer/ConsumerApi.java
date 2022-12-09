package io.confluent.parallelconsumer;

import org.apache.kafka.clients.consumer.Consumer;

/**
 * @author Antony Stubbs
 * @see ConsumerFacade
 */
public interface ConsumerApi {

    void seek(long offset);

    /**
     * Partial implementation of {@link Consumer}
     *
     * @see PartialConsumerApi
     */
    PartialConsumerApi partialKafkaConsumer();
}
