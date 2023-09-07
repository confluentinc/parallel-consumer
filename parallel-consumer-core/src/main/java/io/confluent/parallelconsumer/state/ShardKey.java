package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Objects;

/**
 * Simple value class for processing {@link ShardKey}s to make the various key systems type safe and extendable.
 *
 * @author Antony Stubbs
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
    @EqualsAndHashCode(callSuper = true)
    public static class KeyOrderedKey extends ShardKey {

        /**
         * Note: We use just the topic name here, and not the partition, so that if we were to receive records from the
         * same key from the partitions we're assigned, they will be put into the same queue.
         */
        TopicPartition topicName;

        /**
         * The key of the record being referenced. Nullable if record is produced with a null key.
         */
        KeyWithEquals key;

        public KeyOrderedKey(final ConsumerRecord<?, ?> rec) {
            this(new TopicPartition(rec.topic(), rec.partition()), rec.key());
        }

        public KeyOrderedKey(final TopicPartition topicPartition, final Object key) {
            if (key instanceof KeyWithEquals) {
                this.key = (KeyWithEquals) key;
            } else {
                this.key = new KeyWithEquals(key);
            }
            this.topicName = topicPartition;
        }
    }

    @Value
    @RequiredArgsConstructor
    public static class KeyWithEquals {
        Object key;

        @Override
        public boolean equals(Object o) {

            if (o == this) return true;
            if (!(o instanceof KeyWithEquals)) return false;
            KeyWithEquals other = (KeyWithEquals) o;
            if (other.key == null && this.key == null) return true;
            if (other.key == null || this.key == null) return false;
            return Objects.deepEquals(this.key, other.key);

        }

        @Override
        public int hashCode() {
            final int PRIME = 59;
            int result = 1;
            result = (result * PRIME);
            if (key == null) {
                result = result + 43;
                return result;
            }
            if (isArray(key)) {
                result = result + arrayHashCode(key);
            }
            return result;
        }

        private int arrayHashCode(Object t) {
            if (t instanceof Object[]) {
                return Arrays.deepHashCode((Object[]) t);
            } else {
                return primitiveArrayHashCode(t, t.getClass().getComponentType());
            }
        }

        /**
         * Copy of {@link Arrays#primitiveArrayHashCode} logic
         *
         * @param a
         * @param cl
         * @return
         */
        private int primitiveArrayHashCode(Object a, Class<?> cl) {
            return
                    (cl == byte.class) ? Arrays.hashCode((byte[]) a) :
                            (cl == int.class) ? Arrays.hashCode((int[]) a) :
                                    (cl == long.class) ? Arrays.hashCode((long[]) a) :
                                            (cl == char.class) ? Arrays.hashCode((char[]) a) :
                                                    (cl == short.class) ? Arrays.hashCode((short[]) a) :
                                                            (cl == boolean.class) ? Arrays.hashCode((boolean[]) a) :
                                                                    (cl == double.class) ? Arrays.hashCode((double[]) a) :
                                                                            // If new primitive types are ever added, this method must be
                                                                            // expanded or we will fail here with ClassCastException.
                                                                            Arrays.hashCode((float[]) a);
        }

        private boolean isArray(Object obj) {
            return obj instanceof Object[] || obj instanceof boolean[] ||
                    obj instanceof byte[] || obj instanceof short[] ||
                    obj instanceof char[] || obj instanceof int[] ||
                    obj instanceof long[] || obj instanceof float[] ||
                    obj instanceof double[];
        }
    }


    @Value
    @EqualsAndHashCode(callSuper = true)
    public static class TopicPartitionKey extends ShardKey {
        TopicPartition topicPartition;
    }

}
