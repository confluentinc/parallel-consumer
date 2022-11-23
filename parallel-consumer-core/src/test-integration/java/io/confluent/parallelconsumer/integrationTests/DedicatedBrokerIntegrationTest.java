package io.confluent.parallelconsumer.integrationTests;

import io.confluent.parallelconsumer.integrationTests.utils.ChaosBroker;
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
public class DedicatedBrokerIntegrationTest extends CommonBrokerIntegrationTest {

    @Container
    @Getter(AccessLevel.PROTECTED)
    private final ChaosBroker chaosBroker;

    public DedicatedBrokerIntegrationTest() {
        this.chaosBroker = new ChaosBroker();
    }

    @Override
    protected PCTestBroker getKafkaContainer() {
        return chaosBroker;
    }

}
