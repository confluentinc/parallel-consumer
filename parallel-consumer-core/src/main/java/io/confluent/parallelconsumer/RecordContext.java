package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.ConsumerRecordId;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.*;
import lombok.experimental.Delegate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.Optional;

/**
 * Context information for the wrapped {@link ConsumerRecord}.
 * <p>
 * Includes all accessors (~getters) in {@link ConsumerRecord} via delegation ({@link Delegate}).
 *
 * @see #getNumberOfFailedAttempts()
 */

@Builder(toBuilder = true)
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class RecordContext<K, V> {

    protected final WorkContainer<K, V> workContainer;

    @Getter
    @Delegate
    ConsumerRecord<K, V> consumerRecord;

    public RecordContext(WorkContainer<K, V> wc) {
        this.consumerRecord = wc.getCr();
        this.workContainer = wc;
    }

    /**
     * A useful ID class for consumer records.
     *
     * @return the ID for the contained record
     */
    public ConsumerRecordId getRecordId() {
        var topicPartition = new TopicPartition(topic(), partition());
        return new ConsumerRecordId(topicPartition, offset());
    }

    /**
     * @return the number of times this {@link ConsumerRecord} has failed processing already
     */
    public int getNumberOfFailedAttempts() {
        return workContainer.getNumberOfFailedAttempts();
    }

    public Optional<Instant> getLastFailureAt() {
        return workContainer.getLastFailedAt();
    }
}

