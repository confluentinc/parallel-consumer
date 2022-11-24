package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Order;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

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
public abstract class BrokerIntegrationTest extends CommonBrokerIntegrationTest {

    private static final PCTestBroker kafkaContainer = new PCTestBroker();

//    @Getter(AccessLevel.PROTECTED)
//    private final KafkaClientUtils kcu = new KafkaClientUtils(kafkaContainer);

//    @BeforeEach
//    void open() {
//        kcu.open();
//    }
//
//    @AfterEach
//    void close() {
//        kcu.close();
//    }

    protected List<String> produceMessages(int quantity) {
        return produceMessages(quantity, "");
    }

    @SneakyThrows
    protected List<String> produceMessages(int quantity, String prefix) {
        return kafkaContainer.getKcu().produceMessages(getTopic(), quantity, prefix);
    }

    @Override
    protected PCTestBroker getKafkaContainer() {
        return kafkaContainer;
    }

    @Override
    protected KafkaClientUtils getKcu() {
        return kafkaContainer.getKcu();
    }

}
