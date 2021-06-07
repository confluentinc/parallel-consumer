package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public final class KafkaUtils {

    // todo rename
    public static TopicPartition toTP(ConsumerRecord<?,?> rec) {
        return new TopicPartition(rec.topic(), rec.partition());
    }

}
