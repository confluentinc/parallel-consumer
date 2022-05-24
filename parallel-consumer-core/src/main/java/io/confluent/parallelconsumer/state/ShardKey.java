package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.KeyIsolation;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * todo docs
 */
@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@ToString
@EqualsAndHashCode
public class ShardKey {

    public static ShardKey of(WorkContainer<?, ?> wc, ParallelConsumerOptions options) {
        //return of(wc.getCr(), options.getOrdering(), options.getKeyIsolation());
        final ConsumerRecord<?, ?> rec = wc.getCr();
        return switch (options.getOrdering()) {
            case KEY -> ofKey(rec, options.getKeyIsolation());
            case PARTITION, UNORDERED -> ofTopicPartition(rec);
        };
    }
//
//    public static ShardKey of(ConsumerRecord<?, ?> rec, ProcessingOrder ordering, KeyIsolation isolation) {
//
//    }

    public static KeyOrderedKey ofKey(ConsumerRecord<?, ?> rec, KeyIsolation isolation) {
        return switch (isolation) {
            case ISOLATE -> new KeyIsolated(rec);
            case COMBINE_TOPICS -> new KeyTopicsCombined(rec);
            case COMBINE_PARTITIONS -> new KeyPartitionsCombined(rec);
            default -> throw new IllegalStateException("Unexpected value: " + isolation);
        };
//        return new KeyOrderedKey(isolationObject, rec.key());
    }

    public static ShardKey ofTopicPartition(final ConsumerRecord<?, ?> rec) {
        return new TopicPartitionKey(new TopicPartition(rec.topic(), rec.partition()));
    }

    /**
     * todo docs
     */
    @Getter
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    @FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
    public static class KeyOrderedKey extends ShardKey {
        Object key;

        public KeyOrderedKey(final ConsumerRecord<?, ?> rec) {
//            this(rec.topic(), rec.key());
            this.key = rec.key();
        }
    }


    /**
     * todo docs
     *
     * @see KeyOrderedKey#key
     */
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class KeyTopicsCombined extends KeyOrderedKey {

        // has no extra distinguishing key element bsides super's key

        public KeyTopicsCombined(ConsumerRecord<?, ?> rec) {
            super(rec);
        }
    }

    /**
     * todo docs
     */
    @Getter
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class KeyPartitionsCombined extends KeyOrderedKey {
        /**
         * Note: We use just the topic name here, and not the partition, so that if we were to receive records from the
         * same key from the partitions we're assigned, they will be put into the same queue.
         */
        // todo needs documenting
        private final TopicName topicName;

        public KeyPartitionsCombined(final ConsumerRecord<?, ?> rec) {
            super(rec);
            this.topicName = new TopicName(rec.topic());
        }
    }

    /**
     * todo docs
     */
    @Getter
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class KeyIsolated extends KeyOrderedKey {

        /**
         * Isolate all queues from each other, but incorporating both topic name and partition as shard key
         */
        private final TopicPartition topicPartition;

        public KeyIsolated(final ConsumerRecord<?, ?> rec) {
            super(rec);
            this.topicPartition = new TopicPartition(rec.topic(), rec.partition());
        }
    }

    /**
     * todo docs
     */
    @Value
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    public static class TopicPartitionKey extends ShardKey {
        TopicPartition topicPartition;
    }

    // needs jabel as dep to desugar
    //    @Desugar
    //    private record TopicName(String name) {
    //    }

    /**
     * todo docs
     */
    @Value
    public static class TopicName {
        String name;
    }

}
