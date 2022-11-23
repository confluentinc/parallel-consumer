package io.confluent.parallelconsumer.integrationTests.utils;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.integrationTests.PCTestBroker;
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
import org.apache.kafka.common.config.ConfigResource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.ProducerMode.TRANSACTIONAL;
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
    public final Network network = Network.newNetwork();

    private final GenericContainer<?> zookeeperContainer = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-zookeeper"))
            .withNetwork(network)
            .withNetworkAliases(ZOOKEEPER_NETWORK_ALIAS)
            .withExposedPorts(ZOOKEEPER_PORT)
//            .addExposedPort()
            .withEnv("ZOOKEEPER_CLIENT_PORT", ZOOKEEPER_PORT + "")
//            .withReuse(false)
            .withReuse(true);

    /**
     * https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
     * https://github.com/testcontainers/testcontainers-java/pull/1781
     */
    @Container
    @Getter(AccessLevel.PRIVATE)
    public KafkaContainer kafkaContainer = createKafkaContainer();

    /**
     * Toxiproxy container, which will be used as a TCP proxy
     */
    @Getter
    @Container
    private final ToxiproxyContainer toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(network)
            .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS)
            .dependsOn(kafkaContainer);

    /**
     * Starting proxying connections to a target container
     */
    @Getter
    private final ToxiproxyContainer.ContainerProxy brokerProxy = toxiproxy.getProxy(KAFKA_NETWORK_ALIAS, KAFKA_PROXY_PORT);

    @SneakyThrows
    public ChaosBroker() {
    }

    @Override
    public void start() {
        Startables.deepStart(zookeeperContainer, kafkaContainer, toxiproxy).get();
        updateAdvertisedListenersToProxy();
        followLogs();
    }

    @Override
    public void stop() {
        toxiproxy.stop();
        kafkaContainer.stop();
        zookeeperContainer.stop();
    }

    private KafkaContainer createKafkaContainer() {
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
        var p = new Properties();
        // Kafka Container overrides our env with it's configure method, so have to do some gymnastics
        var boostrapForAdmin = String.format("PLAINTEXT://%s:%s", "localhost", getKafkaContainer().getMappedPort(KAFKA_PORT));
        p.put(BOOTSTRAP_SERVERS_CONFIG, boostrapForAdmin);
        AdminClient admin = AdminClient.create(p);
        admin.incrementalAlterConfigs(Map.of(configResource, List.of(op))).all().get();
        admin.close();

        //
        log.debug("Updated advertised listeners to point to toxi proxy port: {}, advertised listeners: {}", toxiPort, value);
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
    protected void restartDockerUsingCommandsAndProxy() {
        log.debug("Restarting docker container");
//        var oldMapping = StringUtils.joinWith(":", kafkaContainer.getMappedPort(KAFKA_PORT), KAFKA_PORT);
//        var oldMapping = StringUtils.joinWith(":", KAFKA_PORT, kafkaContainer.getMappedPort(KAFKA_PORT));

        var dockerClient = DockerClientFactory.lazyClient();

//        var portBindings = kafkaContainer.getPortBindings();
//
//        String tag = "tag-" + System.currentTimeMillis();
//        var repo = "tmp-kafka-delete-me";
        var kafkaId = kafkaContainer.getContainerId();
//        dockerClient.commitCmd(kafkaId)
//                .withRepository(repo)
//                .withTag(tag)
//                .withPause(true)
////                .withPortSpecs(oldMapping)
////                .withExposedPorts(new ExposedPorts(ExposedPort.parse(KAFKA_PORT + "")))
////                .withVolumes(new Volumes())
//                .exec();

        log.debug("Sending container stop command");
        dockerClient.stopContainerCmd(kafkaId).exec();


//        ThreadUtils.sleepLog(5);

        // zookeeper.session.timeout.ms is 18 seconds by default
//        ThreadUtils.sleepLog(20);

//        var imageNameToUse = StringUtils.joinWith(":", repo, tag);

//        kafkaContainer.setVolumesFroms(List.of());
//        startKafkaContainer();

        //
//        dockerClient.startContainerCmd(kafkaId).exec();


        // have to create a new container
//        kafkaContainer = createKafkaContainer(null);

//        kafkaContainer.setDockerImageName(imageNameToUse);

//        kafkaContainer.setExposedPorts(of()); // remove dynamic mapping in favour of static one


//        kafkaContainer.setPortBindings(of(oldMapping));
//        kafkaContainer.withCreateContainerCmdModifier(cmd -> {
//            var parse = PortBinding.parse(oldMapping);
//            cmd.getHostConfig().withPortBindings(parse);
//        });

        log.debug("Sending container start command");
        dockerClient.startContainerCmd(kafkaId).exec();
//        startKafkaContainer();

        // update proxy upstream
//        updateProxyUpstream();

        //
        log.debug("Finished restarting container: {}", kafkaId);
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
//        proxy.disable();
        proxy.setUpstream(newProxyUpstream);
//        proxy.enable();
    }

    /**
     * Restart the actual broker process inside the docker container
     */
    @SuppressWarnings("resource")
//    @SneakyThrows
    protected void restartBrokerInDocker() {
        log.warn("Restarting broker: Closing broker...");

        var dockerClient = DockerClientFactory.lazyClient();

//        dockerClient.killContainerCmd(kafkaContainer.getContainerId())

//                .withCmd("bash", "-c", "echo 'c' | /opt/kafka/bin/kafka-server-stop.sh")
//                .withAttachStdout(true)
//                .withAttachStderr(true)
//                .exec();

//        dockerClient.execStartCmd(kafkaContainer.getContainerId())
//                .withCmd("bash", "-c", "echo 'c' | /opt/kafka/bin/kafka-server-stop.sh")
//                .withAttachStdout(true)
//                .withAttachStderr(true)
//                .exec(new ExecStartResultCallback(System.out, System.err))
//                .awaitCompletion();
//
//        try {
//            dockerClient.execCreateCmd(kafkaContainer.getContainerId())
////                .withDetach(false)
//                    .withCmd("bash", "-c", "echo 'c' | /usr/bin/kafka-server-stop")
//                    .withAttachStderr(true)
//                    .withAttachStdout(true)
//                    .exec();
////
//            ThreadUtils.sleepLog(1);
//
//            dockerClient.execCreateCmd(kafkaContainer.getContainerId())
////                .withDetach(false)
//                    .withCmd("bash", "-c", "echo 'c' | /etc/confluent/docker/run")
//                    .withAttachStderr(true)
//                    .withAttachStdout(true)
//                    .exec();
//        } catch (Exception e) {
//            log.error("Failed to restart broker", e);
//        }

    }

    /**
     * @see #restoreProxyConnection
     */
    @SneakyThrows
    protected void killProxyConnection() {
        log.warn("Killing first consumer connection via proxy disable"); // todo debug
        Proxy proxy = createProxyControlClient();
        proxy.disable();
    }

    protected KafkaConsumer<String, String> createProxiedConsumer(String groupId) {
        var overridingOptions = new Properties();
        var proxiedBootstrapServers = kcu.getProxiedBootstrapServers();
        overridingOptions.put(BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrapServers);
        return kcu.createNewConsumer(groupId, overridingOptions);
    }

    protected KafkaProducer<String, String> createProxiedTransactionalProducer() {
        var overridingOptions = new Properties();
        var proxiedBootstrapServers = kcu.getProxiedBootstrapServers();
        overridingOptions.put(BOOTSTRAP_SERVERS_CONFIG, proxiedBootstrapServers);
        return kcu.createNewProducer(TRANSACTIONAL, overridingOptions);
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
    protected void restoreProxyConnection() {
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
    protected void simulateBrokerUnreachable() {
        log.warn("Simulating broker connection cut");
        reduceConnectionBandwidthToZero();
    }

    /**
     * @see #simulateBrokerUnreachable()
     */
    protected void simulateBrokerReachable() {
        log.warn("Simulating broker connection recovery");
        restoreConnectionBandwidth();
    }

}
