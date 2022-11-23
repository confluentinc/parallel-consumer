package io.confluent.parallelconsumer.integrationTests.sanity;

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.awaitility.Awaitility.await;

/**
 * Simple tests for container manipulation
 *
 * @author Antony Stubbs
 */
class BrokerIntegrationTestTest extends BrokerIntegrationTest {

    AdminClient admin;

    String clusterId;

    @SneakyThrows
    @Test
    void restartingBroker() {
        admin = createAdmin();

        testConnection();
        restartDockerUsingCommandsAndProxy();
        testConnection();
    }

    private void testConnection() {
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
