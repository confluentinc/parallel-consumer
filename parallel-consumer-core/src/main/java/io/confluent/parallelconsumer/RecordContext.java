package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Context information for the wrapped {@link ConsumerRecord}.
 *
 * @see #getFailureCount()
 */
@AllArgsConstructor
@Builder(toBuilder = true)
@Value
public class RecordContext<K, V> {

    ConsumerRecord<K, V> consumerRecord;

    @Getter(AccessLevel.PACKAGE)
    WorkContainer<K, V> workContainer;

    public RecordContext(WorkContainer<K, V> wc) {
        this.consumerRecord = wc.getCr();
        this.workContainer = wc;
    }

    /**
     * @return the offset of the wrapped record
     * @see ConsumerRecord#offset()
     */
    public long offset() {
        return consumerRecord.offset();
    }

    /**
     * @return the number of times this {@link ConsumerRecord} has failed processing already
     */
    public int getFailureCount() {
        return workContainer.getNumberOfFailedAttempts();
    }
}

