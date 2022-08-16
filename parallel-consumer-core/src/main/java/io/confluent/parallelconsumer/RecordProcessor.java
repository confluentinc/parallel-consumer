package io.confluent.parallelconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

/**
 * Types of user functions used for processing records.
 */
public interface RecordProcessor {

    /**
     * Process a Kafka {@link ConsumerRecord} via {@link PollContext} instances.
     */
    @FunctionalInterface
    interface PollConsumer<K, V> extends java.util.function.Consumer<PollContext<K, V>> {

        /**
         * Process a Kafka {@link ConsumerRecord} via {@link PollContext} instances.
         * <p>
         * User can throw a {@link PCRetriableException}, if an issue is and PC should handle the process of retrying it
         * later. If an exception is thrown that doesn't extend {@link PCRetriableException}, the error will be logged
         * at {@code WARN} level. Note that, by default, any exception thrown from a users function will cause the
         * record to be retried, as if a {@link PCRetriableException} had actually been thrown.
         *
         * @param records the Kafka records to process
         * @see PCRetriableException
         * @see ParallelConsumerOptions#getRetryDelayProvider()
         * @see ParallelConsumerOptions#getDefaultMessageRetryDelay()
         */
        void accept(PollContext<K, V> records);
    }

    @FunctionalInterface
    interface PollConsumerAndProducer<K, V> extends java.util.function.Function<PollContext<K, V>, List<ProducerRecord<K, V>>> {

        /**
         * Like {@link PollConsumer#accept(PollContext)} but also returns records to be produced back to Kafka.
         *
         * @param records the Kafka records to process
         * @return the function result
         * @see PollConsumer#accept(PollContext)
         */
        @Override
        List<ProducerRecord<K, V>> apply(PollContext records);

    }
}
