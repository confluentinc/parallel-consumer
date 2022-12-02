package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.DrainingCloseable;
import io.confluent.parallelconsumer.internal.SubscriptionHandler;
import lombok.Data;
import org.apache.kafka.clients.consumer.KafkaConsumer;

// tag::javadoc[]

/**
 * Asynchronous / concurrent message consumer for Kafka.
 * <p>
 * Currently, there is no direct implementation, only the {@link ParallelStreamProcessor} version (see
 * {@link AbstractParallelEoSStreamProcessor}), but there may be in the future.
 *
 * @param <K> key consume / produce key type
 * @param <V> value consume / produce value type
 * @see AbstractParallelEoSStreamProcessor
 */
// end::javadoc[]
public interface ParallelConsumer<K, V> extends SubscriptionHandler, DrainingCloseable {

    /**
     * Pause this consumer (i.e. stop processing of messages).
     * <p>
     * This operation only has an effect if the consumer is currently running. In all other cases calling this method
     * will be silent a no-op.
     * <p>
     * Once the consumer is paused, the system will stop submitting work to the processing pool. Already submitted in
     * flight work however will be finished. This includes work that is currently being processed inside a user function
     * as well as work that has already been submitted to the processing pool but has not been picked up by a free
     * worker yet.
     * <p>
     * General remarks:
     * <ul>
     * <li>A paused consumer may still keep polling for new work until internal buffers are filled.</li>
     * <li>This operation does not actively pause the subscription on the underlying Kafka Broker (compared to
     * {@link KafkaConsumer#pause KafkaConsumer#pause}).</li>
     * <li>Pending offset commits will still be performed when the consumer is paused.</li>
     * </p>
     */
    void pauseIfRunning();

    /**
     * Resume this consumer (i.e. continue processing of messages).
     * <p>
     * This operation only has an effect if the consumer is currently paused. In all other cases calling this method
     * will be a silent no-op.
     * </p>
     */
    void resumeIfPaused();

    /**
     * A simple tuple structure.
     *
     * @param <L>
     * @param <R>
     */
    @Data
    class Tuple<L, R> {
        private final L left;
        private final R right;

        public static <LL, RR> Tuple<LL, RR> pairOf(LL l, RR r) {
            return new Tuple<>(l, r);
        }
    }

}
