package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.csid.testcontainers.FilteredTestContainerSlf4jLogConsumer;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
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

import java.util.concurrent.ExecutionException;

import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@Slf4j
public abstract class BrokerIntegrationTest<K, V> {

    static {
        System.setProperty("flogger.backend_factory", "com.google.common.flogger.backend.slf4j.Slf4jBackendFactory#getInstance");
    }

    int numPartitions = 1;

    String topic;

    /**
     * https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
     * https://github.com/testcontainers/testcontainers-java/pull/1781
     */
    public static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.0.2"))
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1") //transaction.state.log.replication.factor
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1") //transaction.state.log.min.isr
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1") //transaction.state.log.num.partitions
            // try to speed up initial consumer group formation
            .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500") // group.initial.rebalance.delay.ms default: 3000
            .withReuse(true);

    static {
        kafkaContainer.start();
    }

    protected KafkaClientUtils kcu = new KafkaClientUtils(kafkaContainer);

    @BeforeAll
    static void followKafkaLogs() {
        if (log.isDebugEnabled()) {
            FilteredTestContainerSlf4jLogConsumer logConsumer = new FilteredTestContainerSlf4jLogConsumer(log);
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
        String name = LoadTest.class.getSimpleName();
        setupTopic(name);
    }

    protected String setupTopic(String name) {
        assertThat(kafkaContainer.isRunning()).isTrue(); // sanity

        topic = name + "-" + nextInt();

        ensureTopic(topic, numPartitions);

        return topic;
    }

    protected void ensureTopic(String topic, int numPartitions) {
        NewTopic e1 = new NewTopic(topic, numPartitions, (short) 1);
        CreateTopicsResult topics = kcu.admin.createTopics(UniLists.of(e1));
        try {
            Void all = topics.all().get();
        } catch (ExecutionException e) {
            // fine
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
