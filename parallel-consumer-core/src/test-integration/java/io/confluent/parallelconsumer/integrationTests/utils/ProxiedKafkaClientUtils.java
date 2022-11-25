
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

/**
 * Version of {@link KafkaClientUtils} for use with {@link ChaosBroker} which by default proxies all created clients so
 * that they can reconnect across broker restarts.
 *
 * @author Antony Stubbs
 * @see ChaosBroker#restart()
 */
public class ProxiedKafkaClientUtils extends KafkaClientUtils {

    private final ChaosBroker chaosBroker;

    public ProxiedKafkaClientUtils(ChaosBroker kafkaContainer) {
        super(kafkaContainer);
        this.chaosBroker = kafkaContainer;
    }

    /**
     * @return a proxied admin that will reconnect across restarts
     */
    @Override
    public AdminClient createAdmin() {
        var proxiedBootstrapServers = getProxiedBootstrapServers();
        var properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrapServers);
        return AdminClient.create(properties);
    }

    /**
     * @return a proxied consumer that will reconnect across restarts
     */
    @Override
    public <K, V> KafkaConsumer<K, V> createNewConsumer(String groupId, Properties overridingOptions) {
        var proxiedBootstrapServers = getProxiedBootstrapServers();
        overridingOptions.put(BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrapServers);
        return super.createNewConsumer(groupId, overridingOptions);
    }

    /**
     * @return a proxied producer that will reconnect across restarts
     */
    public <K, V> KafkaProducer<K, V> createNewProducer(ProducerMode mode, Properties overridingOptions) {
        var proxiedBootstrapServers = getProxiedBootstrapServers();
        overridingOptions.put(BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrapServers);
        return super.createNewProducer(mode, overridingOptions);
    }

    private String getProxiedBootstrapServers() {
        return chaosBroker.getProxiedBootstrapServers();
    }

    /**
     * @return a new consumer that does not go through the proxy, and will not be able to reconnect across restarts
     */
    public KafkaConsumer<String, String> createNewDirectConsumer(String groupId) {
        var proxiedBootstrapServers = chaosBroker.getDirectBootstrapServers();
        var overridingOptions = new Properties();
        overridingOptions.put(BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrapServers);
        return super.createNewConsumer(groupId, overridingOptions);
    }
}
