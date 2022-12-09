package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.DrainingCloseable;
import lombok.Data;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Parallel message consumer which also can optionally produce 0 or many {@link ProducerRecord} results to be published
 * back to Kafka.
 *
 * @see #pollAndProduce
 * @see #pollAndProduceMany
 */
public interface ParallelStreamProcessor<K, V> extends ParallelConsumer<K, V>, DrainingCloseable {

    static <KK, VV> ParallelEoSStreamProcessor<KK, VV> createEosStreamProcessor(ParallelConsumerOptions<KK, VV> options) {
        return new ParallelEoSStreamProcessor(options);
    }

    /**
     * Register a function to be applied in parallel to each received message.
     * <p>
     * Throw a {@link PCRetriableException} to retry the message without the system logging an ERROR level message.
     *
     * @param usersVoidConsumptionFunction the function
     */
    // todo why isn't this in ParallelConsumer ?
    void poll(Consumer<PollContext<K, V>> usersVoidConsumptionFunction);


    /**
     * Register a function to be applied in parallel to each received message, which in turn returns one or more
     * {@link ProducerRecord}s to be sent back to the broker.
     * <p>
     * Throw a {@link PCRetriableException} to retry the message without the system logging an ERROR level message.
     *
     * @param callback applied after the produced message is acknowledged by kafka
     */
    void pollAndProduceMany(Function<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction,
                            Consumer<ConsumeProduceResult<K, V, K, V>> callback);

    /**
     * Register a function to be applied in parallel to each received message, which in turn returns one or many
     * {@link ProducerRecord}s to be sent back to the broker.
     * <p>
     * Throw a {@link PCRetriableException} to retry the message without the system logging an ERROR level message.
     */
    void pollAndProduceMany(Function<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction);

    /**
     * Register a function to be applied in parallel to each received message, which in turn returns a
     * {@link ProducerRecord} to be sent back to the broker.
     * <p>
     * Throw a {@link PCRetriableException} to retry the message without the system logging an ERROR level message.
     */
    void pollAndProduce(Function<PollContext<K, V>, ProducerRecord<K, V>> userFunction);

    /**
     * Register a function to be applied in parallel to each received message, which in turn returns a
     * {@link ProducerRecord} to be sent back to the broker.
     * <p>
     * Throw a {@link PCRetriableException} to retry the message without the system logging an ERROR level message.
     *
     * @param callback applied after the produced message is acknowledged by kafka
     */
    void pollAndProduce(Function<PollContext<K, V>, ProducerRecord<K, V>> userFunction,
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
        private final PollContext<K, V> in;
        private final ProducerRecord<KK, VV> out;
        private final RecordMetadata meta;
    }
}
