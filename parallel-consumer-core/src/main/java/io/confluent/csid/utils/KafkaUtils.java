package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Simple identifier tuple for Topic Partitions
 */
@UtilityClass
public final class KafkaUtils {

    public static TopicPartition toTopicPartition(ConsumerRecord<?, ?> rec) {
        return new TopicPartition(rec.topic(), rec.partition());
    }

}
