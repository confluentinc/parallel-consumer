package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import org.apache.kafka.clients.consumer.Consumer;

import java.time.Duration;
import java.util.Collection;

/**
 * The set of implementations of {@link Consumer}, which are implemented, but not in the strict contract of the Kafka
 * Consumer API.
 * <p>
 * Typically, this involves interpreting the intent, and making the equivalent call to PC.
 *
 * @author Antony Stubbs
 * @see PCConsumerAPIStrict
 */
// todo def needs a better name
// todo should this extend PCConsumerAPIStrict or not? If not, should there be another interface which does?
public interface ConsumerFacadeForPC extends PCConsumerAPIStrict {

    /**
     * Like {@link Consumer#pause}, except it pauses ALL PARTITIONS through the PC controller. After enough back
     * pressure is built up, the underlying consumer will pause.
     * <p>
     * Note: this implementation cannot pause individual partitions, only ALL partitions. The parameter is ignored.
     *
     * @param collection ignored
     */
    void pause(Collection<?> collection);

    /**
     * Like {@link Consumer#resume}, except it resumes ALL PARTITIONS through the PC controller. After enough back
     * pressure is released, the underlying consumer will resume.
     * <p>
     * Note: this implementation cannot pause individual partitions, only ALL partitions. The parameter is ignored.
     *
     * @param collection ignored
     */
    void resume(Collection<?> collection);


    void close();

    void close(final Duration timeout);

}
