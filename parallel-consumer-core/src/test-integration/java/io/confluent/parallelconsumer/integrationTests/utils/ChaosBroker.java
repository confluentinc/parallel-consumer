package io.confluent.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.confluent.csid.utils.ThreadUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.jetbrains.annotations.Nullable;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;
import pl.tlinkowski.unij.api.UniMaps;

import java.io.IOException;
import java.util.*;

import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.ProducerMode.TRANSACTIONAL;
import static java.util.concurrent.TimeUnit.SECONDS;
import static lombok.AccessLevel.PRIVATE;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.testcontainers.containers.KafkaContainer.KAFKA_PORT;
import static org.testcontainers.containers.KafkaContainer.ZOOKEEPER_PORT;

/**
 * A broker than can restrict it's connections, restart and die etc.
 * <p>
 * Utilises {@link ToxiproxyContainer} to simulate network issues, and provide stable host bound ports across restarts
 * (see {@link #killProxyConnectionForSomeTime} for details).
 *
 * @author Antony Stubbs
 * @see #killProxyConnectionForSomeTime
 */
@Slf4j
public class ChaosBroker extends PCTestBroker {

    public static final String ZOOKEEPER_NETWORK_ALIAS = "zookeeper";

    public static final String KAFKA_NETWORK_ALIAS = "kafka";

    private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";

    private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0");

    /**
     * Create a common docker network so that containers can communicate with each other
     */
    public Network network;

    /**
     * External Zookeeper which Broker will keep stable across Broker restarts
     */
    private GenericContainer<?> zookeeperContainer;

    /**
     * Toxiproxy container, which will be used as a TCP proxy
     */
    @Getter(PRIVATE)
    private ToxiproxyContainer toxiproxy;

    /**
     * Starting proxying connections to a target container
     */
    @Getter(PRIVATE)
    private ToxiproxyContainer.ContainerProxy brokerProxy;

    /**
     * An admin client that goes through the proxy, and so will automatically reconnect after restarts.
     */
    @Getter(AccessLevel.PUBLIC)
    private AdminClient proxiedAdmin;

    public ChaosBroker(String logSegmentBytes) {
        super(logSegmentBytes);
    }

    @Override
    protected AdminClient getAdmin() {
        return getProxiedAdmin();
    }

    @SneakyThrows
    public ChaosBroker() {
        super();

        initToxiproxy();
    }

    @Override
    protected String getContainerPrefix() {
        return CONTAINER_PREFIX + "chaos-";
    }

    private void initToxiproxy() {
        toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS)
                .dependsOn(kafkaContainer);
    }

    private void initZookeeper() {
        zookeeperContainer = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-zookeeper"))
                .withNetwork(network)
                .withNetworkAliases(ZOOKEEPER_NETWORK_ALIAS)
                .withExposedPorts(ZOOKEEPER_PORT)
                .withEnv("ZOOKEEPER_CLIENT_PORT", ZOOKEEPER_PORT + "")
                .withReuse(false);
    }

    /**
     * Change to not be reusable and use an external zk.
     */
    @Override
    public KafkaContainer createKafkaContainer(@Nullable String logSegmentSize) {
        preKafkaInit();

        return super.createKafkaContainer(null)
                .withReuse(false)

                // so we can restart the broker without restarting zookeeper
                // todo switch to KRAFT instead
                .dependsOn(zookeeperContainer)
                .withExternalZookeeper(getZookeeperConnectionString())

                //
                .withNetwork(network)
                .withNetworkAliases(KAFKA_NETWORK_ALIAS);
    }

    /**
     * tood docs
     */
    @SneakyThrows
    public void setupCompactedEnvironment(String topicToCompact) {
        log.debug("Setting up aggressive compaction...");
        ConfigResource topicConfig = new ConfigResource(ConfigResource.Type.TOPIC, topicToCompact);

        Collection<AlterConfigOp> alterConfigOps = new ArrayList<>();

        alterConfigOps.add(new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT), AlterConfigOp.OpType.SET));
        alterConfigOps.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "1"), AlterConfigOp.OpType.SET));
        alterConfigOps.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0"), AlterConfigOp.OpType.SET));

        var configs = UniMaps.of(topicConfig, alterConfigOps);
        KafkaFuture<Void> all = getKcu().getAdmin().incrementalAlterConfigs(configs).all();
        all.get(5, SECONDS);

        log.debug("Compaction setup complete");
    }

    private void preKafkaInit() {
        initNetwork();
        initZookeeper();
    }

    private void initNetwork() {
        network = Network.newNetwork();
    }

    @SneakyThrows
    @Override
    public void start() {
        zookeeperContainer.start();
        try {
            kafkaContainer.start();
        } catch (Exception e) {
            log.error("Failed to start Kafka container", e);
            throw e;
        }
        toxiproxy.start();

        followLogs();

        postStart();
    }

    private void postStart() {
        setupBrokerProxyAndClients();
        updateAdvertisedListenersToProxy();
        injectContainerPrefix(zookeeperContainer);
        injectContainerPrefix(kafkaContainer);
        injectContainerPrefix(toxiproxy);
    }

    private void setupBrokerProxyAndClients() {
        this.brokerProxy = toxiproxy.getProxy(KAFKA_NETWORK_ALIAS, KAFKA_PROXY_PORT);
        this.proxiedAdmin = createProxiedAdminClient();
        getKcu().open();
    }

    @Override
    public void stop() {
        // in reverse dependence order
        proxiedAdmin.close();
        toxiproxy.stop();
        super.stop();
        zookeeperContainer.stop();
    }

    private String getZookeeperConnectionString() {
        return StringUtils.joinWith(":", ZOOKEEPER_NETWORK_ALIAS, ZOOKEEPER_PORT);
    }

    private void followLogs() {
        if (log.isDebugEnabled()) {
            followContainerLogs(kafkaContainer, "KAFKA");
            followContainerLogs(zookeeperContainer, "ZOOKEEPER");
            followContainerLogs(toxiproxy, "TOXIPROXY");
        }
    }

    /**
     * After the proxy is started, we need to update the advertised listeners to point to the proxy, otherwise the
     * clients will try to connect directly, bypassing the proxy.
     *
     * <pre>
     * LISTENERS: LISTENER_DIRECT://localhost:9093,LISTENER_PROXY://localhost:9094
     * LISTENER_SECURITY_PROTOCOL_MAP: LISTENER_DIRECT:PLAINTEXT,LISTENER_PROXY:PLAINTEXT
     * ADVERTISED_LISTENERS: LISTENER_DIRECT://localhost:9092,LISTENER_PROXY://localhost:${PROXY_PORT}
     * </pre>
     *
     * @see KafkaContainer#containerIsStarted
     */
    @SneakyThrows
    private void updateAdvertisedListenersToProxy() {
        var toxiPort = getBrokerProxy().getProxyPort();

        log.debug("Updating advertised listeners to point to toxi proxy: {}", toxiPort);

        //
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "1");

        // as we only have a single node cluster, only need something here which will satisfy config validation - although
        //  internally the KafkaContainer actually sets the broker to broker listener to something else
        var broker = String.format("BROKER://%s:%s", kafkaContainer.getHost(), KAFKA_INTERNAL_PORT);
        var direct = String.format("PLAINTEXT://%s:%s", "localhost", getKafkaContainer().getMappedPort(KAFKA_PORT));
        var proxy = String.format("LISTENER_PROXY://%s:%s", "localhost", brokerProxy.getProxyPort());

        String value = String.join(",", broker, direct, proxy);
        log.debug("Setting LISTENERS to: {}", value);

        ConfigEntry toxiAdvertListener = new ConfigEntry("advertised.listeners", value);

        AlterConfigOp op = new AlterConfigOp(toxiAdvertListener, AlterConfigOp.OpType.SET);

        // todo reuse existing admin client? or refactor to make construction nicer
        try (AdminClient admin = createDirectAdminClient()) {
            admin.incrementalAlterConfigs(Map.of(configResource, List.of(op))).all().get();
        }

        //
        log.debug("Updated advertised listeners to point to toxi proxy port: {}, advertised listeners: {}", toxiPort, value);
    }

    public String getProxiedBootstrapServers() {
        var bootstraps = String.format("PLAINTEXT://%s:%s", getProxiedHost(), getProxiedKafkaPort());
        return bootstraps;
    }

    private int getProxiedKafkaPort() {
        return brokerProxy.getProxyPort();
    }

    private String getProxiedHost() {
        return brokerProxy.getContainerIpAddress();
    }

    /**
     * Poor man's version of restarting the actual broker. Actually restarting the broker inside docker is difficult.
     */
    protected void killProxyConnectionForSomeTime() {
        killProxyConnection();
        ThreadUtils.sleepLog(20); // long enough to session time out
        restoreProxyConnection();
    }

    /**
     * To maintain a client connection to the broker across the restarts, you need to have connected the client through
     * thr proxy.
     * <p>
     * This is because TestContainers doesn't let use specify the port binding on the host, so we can't bind to the same
     * port again. Instead, we utilise network aliases for the broker, and have the proxy point to the alias. The alias
     * remains constant across restarts, even though the hostname and port changes. And because the proxy shares the
     * same network as the broker, it is able to connect directly to the brokers exposed port (which is also constant),
     * instead of the mapping of it to the host (which changes every start, in order to avoid potential collisions).
     */
    @SneakyThrows
    public void restart() {
        log.warn("Restarting docker container");
        sendStop();
        sendStart();
        log.warn("Finished restarting container");
    }

    /**
     * Stops the docker container, without "stopping" the {@link KafkaContainer}.
     */
    public void sendStop() {
        var dockerClient = DockerClientFactory.lazyClient();
        var kafkaId = kafkaContainer.getContainerId();
        log.warn("Sending container stop command");
        dockerClient.stopContainerCmd(kafkaId).exec();
    }

    /**
     * Starts the docker container, without "starting" the {@link KafkaContainer}.
     */
    public void sendStart() {
        var dockerClient = DockerClientFactory.lazyClient();
        var kafkaId = kafkaContainer.getContainerId();
        log.warn("Sending container start command");
        dockerClient.startContainerCmd(kafkaId).exec();
    }

    /**
     * Updates the proxy upstream to point to the new broker host name. Not needed if using network aliases (see
     * {@link GenericContainer#withNetworkAliases}).
     *
     * @see GenericContainer#withNetworkAliases
     */
    private void updateProxyUpstream() throws IOException {
        var alias = kafkaContainer.getHost();
        log.debug("Updating proxy upstream to point to new container: {} to new alias: {}", kafkaContainer.getContainerId(), alias);
        var newProxyUpstream = StringUtils.joinWith(":", alias, KAFKA_PROXY_PORT);
        var proxy = createProxyControlClient();
        proxy.setUpstream(newProxyUpstream);
    }

    /**
     * @see #restoreProxyConnection
     */
    @SneakyThrows
    public void killProxyConnection() {
        log.warn("Killing first consumer connection via proxy disable"); // todo debug
        Proxy proxy = createProxyControlClient();
        proxy.disable();
    }

    public AdminClient createProxiedAdminClient() {
        var proxiedBootstrapServers = getProxiedBootstrapServers();
        var properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrapServers);
        return AdminClient.create(properties);
    }

    public KafkaConsumer<String, String> createProxiedConsumer(String groupId) {
        var overridingOptions = new Properties();
        var proxiedBootstrapServers = getProxiedBootstrapServers();
        overridingOptions.put(BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrapServers);
        return getKcu().createNewConsumer(groupId, overridingOptions);
    }

    public KafkaProducer<String, String> createProxiedTransactionalProducer() {
        var overridingOptions = new Properties();
        var proxiedBootstrapServers = getProxiedBootstrapServers();
        overridingOptions.put(BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrapServers);
        return getKcu().createNewProducer(TRANSACTIONAL, overridingOptions);
    }

    protected Proxy createProxyControlClient() throws IOException {
        var port = getToxiproxy().getControlPort();
        var host = getToxiproxy().getHost();
        ToxiproxyClient client = new ToxiproxyClient(host, port);
        var proxies = client.getProxies();
        var proxy = proxies.stream().findFirst().get();
        return proxy;
    }

    /**
     * @see #killProxyConnection
     */
    @SneakyThrows
    public void restoreProxyConnection() {
        log.warn("Restore first consumer connection via proxy enable");
        Proxy proxy = createProxyControlClient();
        proxy.enable();
        Thread.sleep(1000);
    }

    /**
     * @see #restoreConnectionBandwidth()
     */
    protected void reduceConnectionBandwidthToZero() {
        log.debug("Reducing connection to zero");
        getBrokerProxy().setConnectionCut(true);
    }

    /**
     * @see #reduceConnectionBandwidthToZero()
     */
    protected void restoreConnectionBandwidth() {
        log.warn("Restore first consumer bandwidth via proxy");
        getBrokerProxy().setConnectionCut(false);
    }

    /**
     * @see #simulateBrokerReachable()
     */
    public void simulateBrokerUnreachable() {
        log.warn("Simulating broker connection cut");
        reduceConnectionBandwidthToZero();
    }

    /**
     * @see #simulateBrokerUnreachable()
     */
    public void simulateBrokerReachable() {
        log.warn("Simulating broker connection recovery");
        restoreConnectionBandwidth();
    }

}
