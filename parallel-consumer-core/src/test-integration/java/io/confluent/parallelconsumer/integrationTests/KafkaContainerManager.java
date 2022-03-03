package io.confluent.parallelconsumer.integrationTests;

import io.confluent.csid.testcontainers.FilteredTestContainerSlf4jLogConsumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.ResourceReaper;

/**
 * Sets up and starts Kafka {@link org.testcontainers.Testcontainers}.
 */
@Slf4j
public class KafkaContainerManager {

    public static final String CONTAINER_PREFIX = "csid-pc-kafka-";

    /**
     * https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
     * https://github.com/testcontainers/testcontainers-java/pull/1781
     */
    @Getter
    private final KafkaContainer kafkaContainer;

    private static KafkaContainerManager singleton;

    private KafkaContainerManager() {
        kafkaContainer = createContainer();
        kafkaContainer.start();
        registerForReaping();
        attachLogger();
    }

    private void registerForReaping() {
        ResourceReaper instance = ResourceReaper.instance();
        instance.registerContainerForCleanup(kafkaContainer.getContainerId(), kafkaContainer.getDockerImageName());
    }

    private KafkaContainer createContainer() {
        return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.1"))
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1") //transaction.state.log.replication.factor
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1") //transaction.state.log.min.isr
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1") //transaction.state.log.num.partitions
                // try to speed up initial consumer group formation
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500") // group.initial.rebalance.delay.ms default: 3000
                .withReuse(true)
                .withCreateContainerCmdModifier(cmd -> cmd.withName(CONTAINER_PREFIX + RandomUtils.nextInt()))
                ;
    }

    private void attachLogger() {
        if (log.isDebugEnabled()) {
            FilteredTestContainerSlf4jLogConsumer logConsumer = new FilteredTestContainerSlf4jLogConsumer(log);
            getKafkaContainer().followOutput(logConsumer);
        }
    }

    public synchronized static KafkaContainerManager getSingleton() {
        if (KafkaContainerManager.singleton == null) {
            KafkaContainerManager.singleton = new KafkaContainerManager();
        }
        return singleton;
    }
}
