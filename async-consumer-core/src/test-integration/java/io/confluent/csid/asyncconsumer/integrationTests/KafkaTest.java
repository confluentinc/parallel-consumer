package io.confluent.csid.asyncconsumer.integrationTests;

import io.confluent.csid.asyncconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Properties;

import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@Slf4j
public abstract class KafkaTest<K, V> {

  int numPartitions = 50; // as default consumer settings are max request 50 max per partition 1 - 50/1=50

  String topic;

  @Container
  static public KafkaContainer kafkaContainer = new KafkaContainer("latest")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1") //transaction.state.log.replication.factor
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1") //transaction.state.log.min.isr
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1"); //transaction.state.log.num.partitions

  KafkaClientUtils kcu = new KafkaClientUtils(kafkaContainer);

  @BeforeAll
  static void followKafkaLogs(){
    if(log.isDebugEnabled()) {
      Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(log);
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
  private void ensureTopic(String topic, int numPartitions) {
    NewTopic e1 = new NewTopic(topic, numPartitions, (short) 1);
    CreateTopicsResult topics = kcu.admin.createTopics(List.of(e1));
    Void all = topics.all().get();
  }

}
