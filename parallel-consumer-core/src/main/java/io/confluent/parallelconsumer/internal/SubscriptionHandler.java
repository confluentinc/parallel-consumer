package io.confluent.parallelconsumer.internal;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collection;
import java.util.regex.Pattern;

/**
 * @author Antony Stubbs
 */
public interface SubscriptionHandler {

    /**
     * @see KafkaConsumer#subscribe(Collection)
     */
    void subscribe(Collection<String> topics);

    /**
     * @see KafkaConsumer#subscribe(Pattern)
     */
    void subscribe(Pattern pattern);

    /**
     * @see KafkaConsumer#subscribe(Collection, ConsumerRebalanceListener)
     */
    void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

    /**
     * @see KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)
     */
    void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

}
