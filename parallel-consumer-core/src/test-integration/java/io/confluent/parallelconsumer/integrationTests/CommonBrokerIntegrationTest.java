package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.integrationTests.utils.PCTestBroker;
import lombok.Getter;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.commons.lang3.RandomUtils.nextInt;

/**
 * Common base for all Broker related integration tests
 *
 * @param <BROKER_TYPE> The type of PCTestBroker to use
 * @author Antony Stubbs
 * @see DedicatedBrokerIntegrationTest
 * @see BrokerIntegrationTest
 */
@Timeout(120)
public abstract class CommonBrokerIntegrationTest<BROKER_TYPE extends PCTestBroker> {

    /**
     * When using {@link org.junit.jupiter.api.Order} to order tests, this is the prefix to use to ensure integration
     * tests run after unit tests.
     */
    public static final int INTEGRATION_TEST_BASE = 100;

    public static final int UNIT_TEST_BASE = 10;

    int numPartitions = 1;

    int partitionNumber = 0;

    @Getter
    String topic;

    static {
        System.setProperty("flogger.backend_factory", "com.google.common.flogger.backend.slf4j.Slf4jBackendFactory#getInstance");
    }

    @BeforeEach
    void open() {
        getKcu().open();
    }

    @AfterEach
    void close() {
        getKcu().close();
    }

    protected String setupTopic() {
        String name = getClass().getSimpleName();
        setupTopic(name);
        return name;
    }

    protected String setupTopic(String name) {
        var kafkaContainer = getKafkaContainer();

        assertThat(kafkaContainer.isRunning()).isTrue(); // sanity

        topic = name + "-" + nextInt();

        kafkaContainer.ensureTopic(topic, numPartitions);

        return topic;
    }

    protected void ensureTopic(String name, int numPartitionsToUse) {
        getKafkaContainer().ensureTopic(name, numPartitionsToUse);
    }

    protected abstract BROKER_TYPE getKafkaContainer();

    protected abstract KafkaClientUtils getKcu();

    protected List<String> produceMessages(int quantity) {
        return produceMessages(quantity, "");
    }

    @SneakyThrows
    protected List<String> produceMessages(int quantity, String prefix) {
        return getKcu().produceMessages(getTopic(), quantity, prefix);
    }

}
