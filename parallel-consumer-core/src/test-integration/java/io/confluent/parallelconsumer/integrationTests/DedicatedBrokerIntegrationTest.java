package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.integrationTests.utils.ChaosBroker;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.AccessLevel;
import lombok.Getter;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * For integration tests which need dedicated brokers, perhaps because they change the brokers state, or shut it down
 * etc.
 *
 * @author Antony Stubbs
 * @see BrokerIntegrationTest for tests which can use a shared, reusable broker
 */
@Testcontainers
public class DedicatedBrokerIntegrationTest extends CommonBrokerIntegrationTest<ChaosBroker> {

    @Getter
    private final KafkaClientUtils kcu;

    @Container
    @Getter(AccessLevel.PROTECTED)
    private final ChaosBroker chaosBroker;

    public DedicatedBrokerIntegrationTest() {
        this.chaosBroker = new ChaosBroker();
        this.kcu = new KafkaClientUtils(chaosBroker);
    }

    @Override
    protected ChaosBroker getKafkaContainer() {
        return chaosBroker;
    }

}
