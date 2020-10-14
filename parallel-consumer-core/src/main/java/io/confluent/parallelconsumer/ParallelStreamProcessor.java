package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.Data;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Parallel message consumer which also can optionally produce 0 or many {@link ProducerRecord} results to be published
 * back to Kafka.
 *
 * @see #pollAndProduce(Function, Consumer)
 */
public interface ParallelStreamProcessor<K, V> extends ParallelConsumer<K, V>, DrainingCloseable {

    static <KK, VV> ParallelStreamProcessor<KK, VV> createEosStreamProcessor(
            org.apache.kafka.clients.consumer.Consumer<KK, VV> consumer,
            org.apache.kafka.clients.producer.Producer<KK, VV> producer,
            ParallelConsumerOptions options) {
        return new ParallelEoSStreamProcessor<>(consumer, producer, options);
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
