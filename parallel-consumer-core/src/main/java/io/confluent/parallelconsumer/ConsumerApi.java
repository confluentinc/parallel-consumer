package io.confluent.parallelconsumer;

import org.apache.kafka.clients.consumer.Consumer;

/**
 * @author Antony Stubbs
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
