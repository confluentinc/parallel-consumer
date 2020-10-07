package io.confluent.parallelconsumer;

import lombok.Data;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @see #pollAndProduce(Function, Consumer)
 */
public interface ParallelStreamProcessor<K, V> extends ParallelConsumer<K, V> {

    static <KK, VV> ParallelStreamProcessor<KK, VV> createEosProcessor(
            org.apache.kafka.clients.consumer.Consumer<KK, VV> c,
            org.apache.kafka.clients.producer.Producer<KK, VV> p,
            ParallelConsumerOptions o) {
        return new ParallelEoSStreamProcessorImpl<>(c, p, o);
    }

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
     * A simple triple structure to capture the set of coinciding data.
     *
     * <ul>
     *     <li>the record consumer</li>
     *     <li>any producer record produced as a result of it's processing</li>
     *     <li>the metadata for publishing that record</li>
     * </ul>
     *
     * @param <K>  in key
     * @param <V>  in value
     * @param <KK> out key
     * @param <VV> out value
     */
    @Data
    class ConsumeProduceResult<K, V, KK, VV> {
        private final ConsumerRecord<K, V> in;
        private final ProducerRecord<KK, VV> out;
        private final RecordMetadata meta;
    }
}
