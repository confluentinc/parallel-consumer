
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests;
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.testcontainers.FilteredTestContainerSlf4jLogConsumer;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;
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
    @Getter
    public static KafkaContainer kafkaContainer = createKafkaContainer();

    public static KafkaContainer createKafkaContainer() {
        return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.1"))
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1") //transaction.state.log.replication.factor
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1") //transaction.state.log.min.isr
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1") //transaction.state.log.num.partitions
                // try to speed up initial consumer group formation
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500") // group.initial.rebalance.delay.ms default: 3000
                .withReuse(true);
    }

    static {
        kafkaContainer.start();
    }

    @Getter
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

    String setupTopic() {
        String name = getClass().getSimpleName();
        return setupTopic(name);
    }

    protected String setupTopic(String name) {
        assertThat(kafkaContainer.isRunning()).isTrue(); // sanity

        topic = name + "-" + nextInt();

        ensureTopic(topic, numPartitions);

        return topic;
    }

    protected void ensureTopic(String topic, int numPartitions) {
        NewTopic e1 = new NewTopic(topic, numPartitions, (short) 1);
        CreateTopicsResult topics = kcu.getAdmin().createTopics(UniLists.of(e1));
        try {
            Void all = topics.all().get();
        } catch (ExecutionException e) {
            // fine
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    int outPort = -1;

    protected void terminateBroker() {
        log.warn(kafkaContainer.getPortBindings().toString());
        log.warn(kafkaContainer.getExposedPorts().toString());
        log.warn(kafkaContainer.getBoundPortNumbers().toString());
        log.warn(kafkaContainer.getLivenessCheckPortNumbers().toString());

        outPort = kafkaContainer.getMappedPort(9093);

        log.debug("Test step: Terminating broker");
//        getKafkaContainer().getDockerClient()
//        String containerId = getKafkaContainer().getContainerId();
//        getKafkaContainer().getDockerClient().killContainerCmd(containerId).exec();
//        Awaitility.await().untilAsserted(() -> assertThat(kafkaContainer.isRunning()).isFalse());


        Network network = kafkaContainer.getNetwork();
        List<com.github.dockerjava.api.model.Network> exec1 = kafkaContainer.getDockerClient().listNetworksCmd().exec();
        com.github.dockerjava.api.model.Network exec = kafkaContainer.getDockerClient().inspectNetworkCmd().exec();
        List<String> networkAliases = kafkaContainer.getNetworkAliases();
        getKafkaContainer().getDockerClient()
                .disconnectFromNetworkCmd()
                .withContainerId(kafkaContainer.getContainerId())
                .withNetworkId(networkAliases.stream().findFirst().get())
                .exec();
    }

    protected void startNewBroker() {
        log.debug("Test step: Starting a new broker");

        String mapping = "9093:" + outPort;
//        BrokerIntegrationTest.kafkaContainer = BrokerIntegrationTest.createKafkaContainer();
//        kafkaContainer.setPortBindings(UniLists.of(mapping));
//        kafkaContainer.start();

//        BrokerIntegrationTest.followKafkaLogs();
//        assertThat(kafkaContainer.isRunning()).isTrue(); // sanity
//        Awaitility.await().untilAsserted(() -> assertThat(kafkaContainer.isRunning()).isTrue());

        getKafkaContainer().getDockerClient().connectToNetworkCmd().exec();

        log.warn(kafkaContainer.getLivenessCheckPortNumbers().toString());
        log.warn(kafkaContainer.getPortBindings().toString());
        log.warn(kafkaContainer.getPortBindings().toString());

    }
}
