package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
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

    public static ShardKey of(WorkContainer<?, ?> wc, ProcessingOrder ordering) {
        return of(wc.getCr(), ordering);
    }

    public static ShardKey of(ConsumerRecord<?, ?> rec, ProcessingOrder ordering) {
        return switch (ordering) {
            case KEY -> ofKey(rec);
            case PARTITION, UNORDERED -> ofTopicPartition(rec);
        };
    }

    public static KeyOrderedKey ofKey(ConsumerRecord<?, ?> rec) {
        return new KeyOrderedKey(rec);
    }

    public static ShardKey ofTopicPartition(final ConsumerRecord<?, ?> rec) {
        return new TopicPartitionKey(new TopicPartition(rec.topic(), rec.partition()));
    }

    @Value
    @RequiredArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class KeyOrderedKey extends ShardKey {
        /**
         * Note: We use just the topic name here, and not the partition, so that if we were to receive records from the
         * same key from the partitions we're assigned, they will be put into the same queue.
         */
        todo need
        to use
        TP actually, so
        that IF
        KEY mode, and
        same keys
        exists on
        multiple partitions, they
        don't overwrite each other, and progress is still made
                -
        need to
        also add
        indirection to
        SM to
        map my
        TP
        String topicName;
        Object key;

        public KeyOrderedKey(final ConsumerRecord<?, ?> rec) {
            this(rec.topic(), rec.key());
        }
    }

    @Value
    @EqualsAndHashCode(callSuper = true)
    public static class TopicPartitionKey extends ShardKey {
        TopicPartition topicPartition;
    }

}
