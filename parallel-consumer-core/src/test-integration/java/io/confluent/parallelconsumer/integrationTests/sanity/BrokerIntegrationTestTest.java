package io.confluent.parallelconsumer.integrationTests.sanity;

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static io.confluent.parallelconsumer.integrationTests.sanity.BrokerIntegrationTestTest.INTEGRATION_TEST_BASE;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.awaitility.Awaitility.await;

/**
 * Simple tests for container manipulation
 *
 * @author Antony Stubbs
 */
// first integration test to make sure integration tests infrastructure is working
@Order(INTEGRATION_TEST_BASE)
@Tag("toxiproxy")
class BrokerIntegrationTestTest extends BrokerIntegrationTest {

    String clusterId;

    @SneakyThrows
    @Test
    void restartingBroker() {
        try (AdminClient admin = createAdmin()) {

            testConnection(admin);
            restartDockerUsingCommandsAndProxy();
            testConnection(admin);
        }
    }

    @Test
    void testConsumerOffsetsPersistAcrossRestartsWhenCommitted() {
        // todo
        throw new UnsupportedOperationException();
    }

    private void testConnection(AdminClient admin) {
        await()
                .ignoreExceptions()
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    var id = admin.describeCluster().clusterId().get();
                    Truth.assertThat(id).isNotEmpty();
                    setBootstrapClusterId(id);
                });
    }

    private void setBootstrapClusterId(String id) {
        if (clusterId == null)
            clusterId = id;
        else
            Truth.assertThat(id).isEqualTo(clusterId);
    }

    private AdminClient createAdmin() {
        var proxiedBootstrapServers = getKcu().getProxiedBootstrapServers();
        var properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrapServers);
        return AdminClient.create(properties);
    }

}
