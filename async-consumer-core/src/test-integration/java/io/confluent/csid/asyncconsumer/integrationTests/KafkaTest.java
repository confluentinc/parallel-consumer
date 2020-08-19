
/*-
 * Copyright (C) 2020 Confluent, Inc.
 */
package io.confluent.csid.asyncconsumer.integrationTests;

import io.confluent.csid.asyncconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.csid.testcontainers.FilteredSlf4jLogConsumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import pl.tlinkowski.unij.api.UniLists;

import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@Slf4j
public abstract class KafkaTest<K, V> {

  int numPartitions = 50; // as default consumer settings are max request 50 max per partition 1 - 50/1=50

  String topic;

  /**
   * https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
   * https://github.com/testcontainers/testcontainers-java/pull/1781
   */
  static public KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1") //transaction.state.log.replication.factor
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1") //transaction.state.log.min.isr
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1") //transaction.state.log.num.partitions
          .withReuse(true);

  static {
    kafkaContainer.start();
  }

  protected KafkaClientUtils kcu = new KafkaClientUtils(kafkaContainer);

  @BeforeAll
  static void followKafkaLogs(){
    if(log.isDebugEnabled()) {
      FilteredSlf4jLogConsumer logConsumer = new FilteredSlf4jLogConsumer(log);
      kafkaContainer.followOutput(logConsumer);
    }
  }

  @BeforeEach
  void open() {
    kcu.open();
  }

  @AfterEach
  void close() {
    kcu.close();
  }

  void setupTopic() {
    assertThat(kafkaContainer.isRunning()).isTrue(); // sanity

    topic = LoadTest.class.getSimpleName() + nextInt(0, 1000);

    ensureTopic(topic, numPartitions);
  }

  @SneakyThrows
  protected void ensureTopic(String topic, int numPartitions) {
    NewTopic e1 = new NewTopic(topic, numPartitions, (short) 1);
    CreateTopicsResult topics = kcu.admin.createTopics(UniLists.of(e1));
    Void all = topics.all().get();
  }

}
