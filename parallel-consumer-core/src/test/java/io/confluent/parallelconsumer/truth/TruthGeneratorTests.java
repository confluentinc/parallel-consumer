package io.confluent.parallelconsumer.truth;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.PodamUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.model.CommitHistory;
import io.confluent.parallelconsumer.state.PartitionState;
import io.stubbs.truth.generator.SourceClassSets;
import io.stubbs.truth.generator.TruthGeneratorAPI;
import io.stubbs.truth.generator.internal.MyStringSubject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

class TruthGeneratorTests {

    @Test
    void generate(@TempDir Path tempDir) {
        TruthGeneratorAPI tg = TruthGeneratorAPI.create(tempDir);
        tg.registerStandardSubjectExtension(String.class, MyStringSubject.class);
        SourceClassSets ss = new SourceClassSets(CommitHistory.class);

        //
        ss.generateAllFoundInPackagesOf(PartitionState.class);

        //
        ss.generateFrom(ParallelConsumerOptions.class);

        // future support for non-bean classes
        ss.generateFromShadedNonBean(ConsumerRecord.class,
                ConsumerRecords.class,
                ProducerRecord.class,
                OffsetAndMetadata.class,
                TopicPartition.class,
                RecordMetadata.class);

        tg.generate(ss);

        ConsumerRecord actual = PodamUtils.createInstance(ConsumerRecord.class, String.class, String.class);
//    ConsumerRecordChildSubject.assertTruth(actual).hasTopic();
//    ManagedTruth.assertTruth(PodamUtils.createInstance(ConsumerRecords.class)).hasPartitions();
//
//    ManagedTruth.assertTruth(PodamUtils.createInstance(OffsetAndMetadata.class));
//
//    ManagedTruth.assertTruth(PodamUtils.createInstance(TopicPartition.class));
//
//    ManagedTruth.assertTruth(PodamUtils.createInstance(RecordMetadata.class));
//
//    ManagedTruth.assertTruth(PodamUtils.createInstance(ProducerRecord.class));

    }

}
