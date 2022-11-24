package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.integrationTests.utils.PCTestBroker;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Order;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static io.confluent.parallelconsumer.integrationTests.CommonBrokerIntegrationTest.INTEGRATION_TEST_BASE;

/**
 * Adding {@link Container} to the containers causes them to be closed after the test, which we don't want if we're
 * sharing kafka instances between tests for performance.
 *
 * @author Antony Stubbs
 * @see DedicatedBrokerIntegrationTest
 */
// first integration test to make sure integration tests infrastructure is working
@Order(INTEGRATION_TEST_BASE)
@Testcontainers
@Slf4j
public abstract class BrokerIntegrationTest extends CommonBrokerIntegrationTest<PCTestBroker> {

    private static final PCTestBroker kafkaContainer = new PCTestBroker();

    /**
     * Not using {@link Container} lifecycle, as we want to reuse the container between tests - the Container lifecycle
     * plugin closes the container, regardless of whether it's marked for reuse or not.
     */
    static {
        kafkaContainer.start();
    }

    @Getter
    private final KafkaClientUtils kcu = new KafkaClientUtils(kafkaContainer);

    @Override
    protected PCTestBroker getKafkaContainer() {
        return kafkaContainer;
    }

}
