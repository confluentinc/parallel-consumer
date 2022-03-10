package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * TODO docs
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

    public long offset() {
        return consumerRecord.offset();
    }

    public int getFailureCount() {
        return workContainer.getNumberOfFailedAttempts();
    }
}

