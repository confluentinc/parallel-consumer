package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.ConsumerFacadeForPC;
import io.confluent.parallelconsumer.internal.ConsumerFacadeStrictImpl;
import io.confluent.parallelconsumer.internal.FullConsumerFacadeImpl;
import io.confluent.parallelconsumer.internal.PCConsumerAPIStrict;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * Get access to the underlying {@link Consumer} interface.
 *
 * @author Antony Stubbs
 * @see FullConsumerFacadeImpl
 * @see PCConsumerAPIStrict
 * @see ConsumerFacadeStrictImpl
 */
public interface ConsumerApiAccess<K, V> {

    /**
     * All consumer style methods, including ones that can't run. For which, you can either choose to no-op or throw an
     * exception.
     * todo docs
     * <p>
     * An implementation of the Kafka Consumer API, with unsupported functions which throw exceptions. Useful for when you
     * want to let something else use PC, where it's unaware of it's capabilities, yet you know it won't call unsupported
     * methods.
     *
     * @see FullConsumerFacadeImpl
     * @see #partialStrictKafkaConsumer()
     */
    FullConsumerFacadeImpl<K, V> fullConsumerFacade();

    /**
     * The {@link PCConsumerAPIStrict} APIs, plus a set of implementations of {@link Consumer}, which are implemented,
     * but not in the strict contract of the Kafka Consumer API.
     *
     * @see ConsumerFacadeForPC
     */
    // todo def needs a better name
    ConsumerFacadeForPC consumerFacadeForPC();

    /**
     * Partial implementation of {@link Consumer}, for which methods are implemented in teh strict sense of the
     * contract.
     * <p>
     * Use this if possible instead of {@link #fullConsumerFacade()}, as it only has the functions that are allowed to
     * be called.
     * <p>
     * Use {@link #fullConsumerFacade()} if you need something that "is a"
     * {@link org.apache.kafka.clients.consumer.Consumer}, but won't have unsupported functions called.
     *
     * @see PCConsumerAPIStrict
     */
    PCConsumerAPIStrict partialStrictKafkaConsumer();
}
