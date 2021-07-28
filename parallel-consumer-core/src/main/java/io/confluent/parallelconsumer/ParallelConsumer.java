package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.DrainingCloseable;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.regex.Pattern;

// tag::javadoc[]

/**
 * Asynchronous / concurrent message consumer for Kafka.
 * <p>
 * Currently there is no direct implementation, only the {@link ParallelStreamProcessor} version (see {@link
 * AbstractParallelEoSStreamProcessor}), but there may be in the future.
 *
 * @param <K> key consume / produce key type
 * @param <V> value consume / produce value type
 * @see AbstractParallelEoSStreamProcessor
 * @see #poll(Consumer)
 */
// end::javadoc[]
public interface ParallelConsumer<K, V> extends DrainingCloseable {

    /**
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(Collection)
     */
    void subscribe(Collection<String> topics);

    /**
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(Pattern)
     */
    void subscribe(Pattern pattern);

    /**
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(Collection, ConsumerRebalanceListener)
     */
    void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

    /**
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)
     */
    void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

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
