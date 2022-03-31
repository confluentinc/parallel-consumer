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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.tlinkowski.unij.api.UniMaps;

import java.nio.file.Path;

import static io.confluent.parallelconsumer.ManagedTruth.assertTruth;

class TruthGeneratorTests {

    @Test
    void generate(@TempDir Path tempDir) {
        // todo check legacy's also contribute to subject graph
        assertTruth(new ConsumerRecords<>(UniMaps.of())).getPartitions().isEmpty();

        assertTruth(PodamUtils.createInstance(OffsetAndMetadata.class)).hasOffsetEqualTo(1);

        assertTruth(PodamUtils.createInstance(TopicPartition.class)).hasTopic().isNotEmpty();

        assertTruth(PodamUtils.createInstance(RecordMetadata.class)).ishasTimestamp();

        assertTruth(PodamUtils.createInstance(ProducerRecord.class, String.class, String.class)).getHeaders().isEmpty();
    }

}
