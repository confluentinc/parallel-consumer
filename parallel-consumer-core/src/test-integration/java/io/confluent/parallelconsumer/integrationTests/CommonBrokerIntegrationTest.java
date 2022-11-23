package io.confluent.parallelconsumer.integrationTests;

/**
 * @author Antony Stubbs
 */
public class CommonBrokerIntegrationTest {

    /**
     * When using {@link org.junit.jupiter.api.Order} to order tests, this is the prefix to use to ensure integration
     * tests run after unit tests.
     */
    public static final int INTEGRATION_TEST_BASE = 100;

}
