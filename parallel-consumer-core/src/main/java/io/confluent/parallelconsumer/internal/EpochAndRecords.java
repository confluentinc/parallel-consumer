package io.confluent.parallelconsumer.internal;

import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRecords;

@Value
public class EpochAndRecords<K, V> {
    ConsumerRecords<K, V> poll;
    long myEpoch;

    public EpochAndRecords(ConsumerRecords<K, V> poll, long epoch) {
        this.poll = poll;
        this.myEpoch = epoch;
    }
}
