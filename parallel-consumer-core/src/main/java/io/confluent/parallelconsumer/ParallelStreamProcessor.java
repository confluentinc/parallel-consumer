package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
import io.confluent.parallelconsumer.internal.DrainingCloseable;
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
 * @see #pollAndProduce
 * @see #pollAndProduceMany
 */
public interface ParallelStreamProcessor<K, V> extends ParallelConsumer<K, V>, DrainingCloseable {

    static <KK, VV> ParallelStreamProcessor<KK, VV> createEosStreamProcessor(ParallelConsumerOptions<KK, VV> options) {
        return new ParallelEoSStreamProcessor(options);
    }

    /**
     * Register a function to be applied in parallel to each received message
     *
     * @param usersVoidConsumptionFunction the function
     */
    void poll(Consumer<ConsumerRecord<K, V>> usersVoidConsumptionFunction);


    /**
     * Register a function to be applied in parallel to each received message, which in turn returns one or more {@link
     * ProducerRecord}s to be sent back to the broker.
     *
     * @param callback applied after the produced message is acknowledged by kafka
     */
    @SneakyThrows
    void pollAndProduceMany(Function<ConsumerRecord<K, V>, List<ProducerRecord<K, V>>> userFunction,
                            Consumer<ConsumeProduceResult<K, V, K, V>> callback);

    /**
     * Register a function to be applied in parallel to each received message, which in turn returns one or many {@link
     * ProducerRecord}s to be sent back to the broker.
     */
    @SneakyThrows
    void pollAndProduceMany(Function<ConsumerRecord<K, V>, List<ProducerRecord<K, V>>> userFunction);

    /**
     * Register a function to be applied in parallel to each received message, which in turn returns a {@link
     * ProducerRecord} to be sent back to the broker.
     */
    @SneakyThrows
    void pollAndProduce(Function<ConsumerRecord<K, V>, ProducerRecord<K, V>> userFunction);

    /**
     * Register a function to be applied in parallel to each received message, which in turn returns a {@link
     * ProducerRecord} to be sent back to the broker.
     *
     * @param callback applied after the produced message is acknowledged by kafka
     */
    @SneakyThrows
    void pollAndProduce(Function<ConsumerRecord<K, V>, ProducerRecord<K, V>> userFunction,
                        Consumer<ConsumeProduceResult<K, V, K, V>> callback);

    /**
     * Register a function to be applied to a batch of messages.
     * <p>
     * The system will treat the messages as a set, so if an error is thrown by the user code, then all messages will be
     * marked as failed and be retried (Note that when they are retried, there is no guarantee they will all be in the
     * same batch again). So if you're going to process messages individually, then don't use this function.
     * <p>
     * Otherwise, if you're going to process messages in sub sets from this batch, it's better to instead adjust the
     * {@link ParallelConsumerOptions#getBatchSize()} instead to the actual desired size, and process them as a whole.
     *
     * @see ParallelConsumerOptions#getBatchSize()
     */
    void pollBatch(Consumer<List<ConsumerRecord<K, V>>> usersVoidConsumptionFunction);

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
