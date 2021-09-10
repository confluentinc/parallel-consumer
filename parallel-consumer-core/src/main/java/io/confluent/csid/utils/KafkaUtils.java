package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class KafkaUtils {
    public static TopicPartition toTP(ConsumerRecord rec) {
        return new TopicPartition(rec.topic(), rec.partition());
    }
}
