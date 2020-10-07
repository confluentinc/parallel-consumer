package io.confluent.parallelconsumer;

import lombok.Data;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Asynchronous / concurrent message consumer for Kafka.
 *
 * @param <K> key consume / produce key type
 * @param <V> value consume / produce value type
 * @see #poll(Consumer)
 * @see #pollAndProduce(Function, Consumer)
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
     * Register a function to be applied in parallel to each received message, which in turn returns a {@link
     * ProducerRecord} to be sent back to the broker.
     *
     * @param callback applied after the produced message is acknowledged by kafka
     */
    @SneakyThrows
    void pollAndProduce(Function<ConsumerRecord<K, V>, List<ProducerRecord<K, V>>> userFunction,
                        Consumer<ConsumeProduceResult<K, V, K, V>> callback);

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

    /**
     * A simple triple structure to capture the set of coinciding data.
     *
     * <ul>
     *     <li>the record consumer</li>
     *     <li>any producer record produced as a result of it's procssing</li>
     *     <li>the metadata for publishing that record</li>
     * </ul>
     *
     * @param <K>  in key
     * @param <V>  in value
     * @param <KK> out key
     * @param <VV> out value
     */
    @Data
    public static class ConsumeProduceResult<K, V, KK, VV> {
        final private ConsumerRecord<K, V> in;
        final private ProducerRecord<KK, VV> out;
        final private RecordMetadata meta;
    }
}
