package io.confluent.parallelconsumer.integrationTests;

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.Getter;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.commons.lang3.RandomUtils.nextInt;

/**
 * @author Antony Stubbs
 * @see DedicatedBrokerIntegrationTest
 * @see BrokerIntegrationTest
 */
public abstract class CommonBrokerIntegrationTest {

    /**
     * When using {@link org.junit.jupiter.api.Order} to order tests, this is the prefix to use to ensure integration
     * tests run after unit tests.
     */
    public static final int INTEGRATION_TEST_BASE = 100;

    int numPartitions = 1;

    int partitionNumber = 0;

    @Getter
    String topic;

    static {
        System.setProperty("flogger.backend_factory", "com.google.common.flogger.backend.slf4j.Slf4jBackendFactory#getInstance");
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

    protected abstract PCTestBroker getKafkaContainer();

    protected abstract KafkaClientUtils getKcu();

}
