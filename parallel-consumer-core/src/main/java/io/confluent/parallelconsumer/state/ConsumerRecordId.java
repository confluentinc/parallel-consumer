package io.confluent.parallelconsumer.state;

import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Useful identifier for a {@link ConsumerRecord}.
 */
@Value
public class ConsumerRecordId {
    TopicPartition tp;
    long offset;
}
