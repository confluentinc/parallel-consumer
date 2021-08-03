package io.confluent.parallelconsumer.truth;

import com.google.common.truth.extension.generator.SourceClassSets;
import com.google.common.truth.extension.generator.TruthGeneratorAPI;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.model.CommitHistory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

class TruthGeneratorTests {

  @Test
  void generate() {
    TruthGeneratorAPI tg = TruthGeneratorAPI.create();
    SourceClassSets ss = new SourceClassSets(CommitHistory.class);
    ss.generateAllFoundInPackagesOf(CommitHistory.class);
    ss.generateAllFoundInPackagesOf(ParallelConsumerOptions.class);


    // future support for non-bean classes
    ss.generateFromShaded(ConsumerRecord.class,
            ConsumerRecords.class,
            ProducerRecord.class,
            OffsetAndMetadata.class);

    tg.generate(ss);
  }
}
