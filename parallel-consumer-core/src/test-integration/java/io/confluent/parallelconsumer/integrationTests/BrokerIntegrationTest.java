package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.testcontainers.FilteredTestContainerSlf4jLogConsumer;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.commons.lang3.RandomUtils.nextInt;

/**
 * @author Antony Stubbs
 */
@Testcontainers
@Slf4j
public abstract class BrokerIntegrationTest<K, V> {

    static {
        System.setProperty("flogger.backend_factory", "com.google.common.flogger.backend.slf4j.Slf4jBackendFactory#getInstance");
    }

    int numPartitions = 1;
    int partitionNumber = 0;

    @Getter
    String topic;

    /**
     * https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
     * https://github.com/testcontainers/testcontainers-java/pull/1781
     */
    @Getter(AccessLevel.PROTECTED)
    public static KafkaContainer kafkaContainer = createKafkaContainer(null);

    public static KafkaContainer createKafkaContainer(String logSegmentSize) {
        KafkaContainer base = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.2"))
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1") //transaction.state.log.replication.factor
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1") //transaction.state.log.min.isr
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1") //transaction.state.log.num.partitions
                //todo need to customise this for this test
                // default produce batch size is - must be at least higher than it: 16KB
                // try to speed up initial consumer group formation
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500") // group.initial.rebalance.delay.ms default: 3000
                .withReuse(true);

        if (StringUtils.isNotBlank(logSegmentSize)) {
            base = base.withEnv("KAFKA_LOG_SEGMENT_BYTES", logSegmentSize);
        }

        return base;
    }

    static {
        kafkaContainer.start();
    }

    @Getter(AccessLevel.PROTECTED)
    private final KafkaClientUtils kcu = new KafkaClientUtils(kafkaContainer);

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

    protected void setupTopic() {
        String name = getClass().getSimpleName();
        setupTopic(name);
    }

    protected String setupTopic(String name) {
        assertThat(kafkaContainer.isRunning()).isTrue(); // sanity

        topic = name + "-" + nextInt();

        ensureTopic(topic, numPartitions);

        return topic;
    }

    protected CreateTopicsResult ensureTopic(String topic, int numPartitions) {
        NewTopic e1 = new NewTopic(topic, numPartitions, (short) 1);
        CreateTopicsResult topics = kcu.getAdmin().createTopics(UniLists.of(e1));
        try {
            Void all = topics.all().get(1, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            // fine
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return topics;
    }

    protected List<String> produceMessages(int quantity) {
        return produceMessages(quantity, "");
    }

    @SneakyThrows
    protected List<String> produceMessages(int quantity, String prefix) {
        return getKcu().produceMessages(getTopic(), quantity, prefix);
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
