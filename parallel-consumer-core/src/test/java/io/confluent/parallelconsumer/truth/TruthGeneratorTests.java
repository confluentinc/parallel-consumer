package io.confluent.parallelconsumer.truth;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import com.google.common.truth.extension.generator.SourceClassSets;
import com.google.common.truth.extension.generator.TruthGeneratorAPI;
import com.google.common.truth.extension.generator.internal.MyStringSubject;
import io.confluent.csid.utils.InstanceUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.model.CommitHistory;
import io.confluent.parallelconsumer.model.ManagedTruth;
import io.confluent.parallelconsumer.model.shaded.org.apache.kafka.clients.consumer.ConsumerRecordChildSubject;
import io.confluent.parallelconsumer.state.PartitionState;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class TruthGeneratorTests {

  @Test
  void generate() {
    TruthGeneratorAPI tg = TruthGeneratorAPI.create();
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

    ConsumerRecord actual = InstanceUtils.createInstance(ConsumerRecord.class, String.class, String.class);
    ConsumerRecordChildSubject.assertTruth(actual).hasTopic();
    ManagedTruth.assertTruth(InstanceUtils.createInstance(ConsumerRecords.class)).hasPartitions();

    ManagedTruth.assertTruth(InstanceUtils.createInstance(OffsetAndMetadata.class));

    ManagedTruth.assertTruth(InstanceUtils.createInstance(TopicPartition.class));

    ManagedTruth.assertTruth(InstanceUtils.createInstance(RecordMetadata.class));

    ManagedTruth.assertTruth(InstanceUtils.createInstance(ProducerRecord.class));

  }

}
