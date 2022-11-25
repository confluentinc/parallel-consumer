
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests.sanity;

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.integrationTests.DedicatedBrokerIntegrationTest;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;

import java.time.Duration;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.integrationTests.sanity.ChaosBrokerTest.INTEGRATION_TEST_BASE;
import static io.confluent.parallelconsumer.integrationTests.utils.PCTestBroker.CONTAINER_PREFIX;
import static org.awaitility.Awaitility.await;

/**
 * Simple tests for container manipulation
 *
 * @author Antony Stubbs
 */
@Order(INTEGRATION_TEST_BASE - 10)
@Tag("toxiproxy")
class ChaosBrokerTest extends DedicatedBrokerIntegrationTest {

    String clusterId;

    @SneakyThrows
    @Test
    void restartingBroker() {
        try (AdminClient admin = getKcu().createAdmin()) {
            testConnection(admin);
            getChaosBroker().restart();
            testConnection(admin);
        }
    }

    @Test
    void containerPrefixInjection() {
        var details = DockerClientFactory.lazyClient().inspectContainerCmd(getChaosBroker().getKafkaContainerId()).exec();
        var containerName = details.getName();
        assertThat(containerName).startsWith("/" + CONTAINER_PREFIX);
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

}
