package io.confluent.parallelconsumer.truth;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.PodamUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniMaps;

import static io.confluent.parallelconsumer.ManagedTruth.assertTruth;

/**
 * Basic tests of simple usage of the Truth Generator maven plugin
 */
class TruthGeneratorTests {

    @Test
    @Disabled("Temporary issues with truth plugin")
    void generate() {
        // todo check legacy's also contribute to subject graph
        assertTruth(new ConsumerRecords<>(UniMaps.of())).getPartitions().isEmpty();

        assertTruth(PodamUtils.createInstance(OffsetAndMetadata.class)).hasOffsetEqualTo(1);

        assertTruth(PodamUtils.createInstance(TopicPartition.class)).hasTopic().isNotEmpty();

        assertTruth(PodamUtils.createInstance(RecordMetadata.class)).ishasTimestamp();

        assertTruth(PodamUtils.createInstance(ProducerRecord.class, String.class, String.class)).getHeaders().isEmpty();
    }

}
