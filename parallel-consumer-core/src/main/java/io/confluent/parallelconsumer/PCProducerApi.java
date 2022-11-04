package io.confluent.parallelconsumer;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author Antony Stubbs
 */
public interface PCProducerApi<K, V> {
    void sendToLeastLoaded(ProducerRecord<K, V> makeRecord);
}
