package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRecords;

@Value
public class EpochAndRecords<K, V> {
    ConsumerRecords<K, V> consumerRecs;
    long myEpoch;

    public EpochAndRecords(ConsumerRecords<K, V> poll, long epoch) {
        this.consumerRecs = poll;
        this.myEpoch = epoch;
    }
}
