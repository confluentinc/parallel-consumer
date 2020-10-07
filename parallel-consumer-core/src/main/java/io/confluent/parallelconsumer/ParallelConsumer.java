package io.confluent.parallelconsumer;

import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Asynchronous / concurrent message consumer for Kafka.
 *
 * @param <K> key consume / produce key type
 * @param <V> value consume / produce value type
 * @see #poll(Consumer)
 */
public interface ParallelConsumer<K, V> {

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
     * Register a function to be applied in parallel to each received message
     *
     * @param usersVoidConsumptionFunction the function
     */
    void poll(Consumer<ConsumerRecord<K, V>> usersVoidConsumptionFunction);

    /**
     * Close the consumer.
     * <p>
     * Uses a default timeout.
     *
     * @see #close(Duration, boolean)
     */
    void close();

    /**
     * TODO docs
     *
     * @param waitForInflight
     */
    void close(boolean waitForInflight);

    /**
     * Close the consumer.
     *
     * @param timeout         how long to wait before giving up
     * @param waitForInFlight wait for messages already consumed from the broker to be processed before closing
     */
    void close(Duration timeout, boolean waitForInFlight);

    /**
     * Of the records consumed from the broker, how many do we have remaining in our local queues
     *
     * @return the number of consumed but outstanding records to process
     */
    int workRemaining();

    /**
     * A simple tuple structure.
     *
     * @param <L>
     * @param <R>
     */
    @Data
    public static class Tuple<L, R> {
        final private L left;
        final private R right;

        public static <LL, RR> Tuple<LL, RR> pairOf(LL l, RR r) {
            return new Tuple<>(l, r);
        }
    }

}
