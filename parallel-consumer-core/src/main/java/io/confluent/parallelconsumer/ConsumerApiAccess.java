package io.confluent.parallelconsumer;

import io.confluent.parallelconsumer.internal.ConsumerFacade;
import io.confluent.parallelconsumer.internal.FullConsumerFacade;
import io.confluent.parallelconsumer.internal.PCConsumerAPI;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * Get access to the underlying {@link Consumer} interface.
 *
 * @author Antony Stubbs
 * @see FullConsumerFacade
 * @see PCConsumerAPI
 * @see ConsumerFacade
 */
public interface ConsumerApiAccess<K, V> {

    /**
     * todo docs
     * <p>
     * An implementation of the Kafka Consumer API, with unsupported functions which throw exceptions. Useful for when you
     * want to let something else use PC, where it's unaware of it's capabilities, yet you know it won't call unsupported
     * methods.
     *
     * @see FullConsumerFacade
     * @see #partialKafkaConsumer()
     */
    FullConsumerFacade<K, V> fullConsumerFacade();

    /**
     * Partial implementation of {@link Consumer}.
     * <p>
     * Use this if possible instead of {@link #fullConsumerFacade()}, as it only has the functions that are allowed to
     * be called.
     * <p>
     * Use {@link #fullConsumerFacade()} if you need something that "is a"
     * {@link org.apache.kafka.clients.consumer.Consumer}, but won't have unsupported functions called.
     *
     * @see PCConsumerAPI
     * @see ConsumerFacade
     * @see #fullConsumerFacade()
     * @see PCConsumerAPI
     */
    PCConsumerAPI partialKafkaConsumer();
}
