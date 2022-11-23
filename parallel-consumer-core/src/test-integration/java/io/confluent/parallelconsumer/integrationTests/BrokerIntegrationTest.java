package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.confluent.csid.testcontainers.FilteredTestContainerSlf4jLogConsumer;
import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.ProducerMode.TRANSACTIONAL;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.testcontainers.containers.KafkaContainer.KAFKA_PORT;
import static org.testcontainers.containers.KafkaContainer.ZOOKEEPER_PORT;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Adding {@link Container} to the containers causes them to be closed after the test, which we don't want if we're
 * sharing kafka instances between tests for performance.
 *
 * @author Antony Stubbs
 */
@Testcontainers
@Slf4j
public abstract class BrokerIntegrationTest {

    private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";

    public static final int KAFKA_INTERNAL_PORT = KAFKA_PORT - 1;

    public static final int KAFKA_PROXY_PORT = KAFKA_PORT + 1;
    public static final String KAFKA_NETWORK_ALIAS = "kafka";

    int numPartitions = 1;

    int partitionNumber = 0;

    @Getter
    String topic;

    /**
     * Create a common docker network so that containers can communicate with each other
     */
    public static Network network = Network.newNetwork();

    public static final String ZOOKEEPER_NETWORK_ALIAS = "zookeeper";
    private static final GenericContainer<?> zookeeperContainer = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-zookeeper"))
            .withNetwork(network)
            .withNetworkAliases(ZOOKEEPER_NETWORK_ALIAS)
            .withExposedPorts(ZOOKEEPER_PORT)
//            .addExposedPort()
            .withReuse(true)
            .withEnv("ZOOKEEPER_CLIENT_PORT", ZOOKEEPER_PORT + "")
//            .withReuse(false)
            ;

    static {
        zookeeperContainer.start();
    }

    static {
        FilteredTestContainerSlf4jLogConsumer logConsumer = new FilteredTestContainerSlf4jLogConsumer(log);
        logConsumer.withPrefix("ZOOKEEPER");
        zookeeperContainer.followOutput(logConsumer);
    }


    /**
     * https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
     * https://github.com/testcontainers/testcontainers-java/pull/1781
     */
    // todo resolve
//    @Container
    @Getter(AccessLevel.PROTECTED)
    public static KafkaContainer kafkaContainer = createKafkaContainer(null);

    /**
     * @see #updateAdvertisedListenersToProxy()
     */
    public static KafkaContainer createKafkaContainer(String logSegmentSize) {
        KafkaContainer base = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.2"))
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1") //transaction.state.log.replication.factor
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1") //transaction.state.log.min.isr
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1") //transaction.state.log.num.partitions
                //todo need to customise this for this test
                // default produce batch size is - must be at least higher than it: 16KB
                // try to speed up initial consumer group formation
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500") // group.initial.rebalance.delay.ms default: 3000
                .withEnv("KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS", "5000") // group.max.session.timeout.ms default: 300000
                .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "5000") // group.min.session.timeout.ms default: 6000
                //
                .withEnv("KAFKA_LISTENERS", msg("BROKER://0.0.0.0:{},PLAINTEXT://0.0.0.0:{},LISTENER_PROXY://0.0.0.0:{}",
                        KAFKA_INTERNAL_PORT, KAFKA_PORT, KAFKA_PROXY_PORT
                )) // BROKER listener is implicit
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,LISTENER_PROXY:PLAINTEXT")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", msg("BROKER://localhost:{},PLAINTEXT://localhost:{},LISTENER_PROXY://localhost:{}",
                        KAFKA_INTERNAL_PORT, KAFKA_PORT, KAFKA_PROXY_PORT
                )) // gets updated later

                // so we can restart the broker without restarting zookeeper
                // todo switch to KRAFT instead
                .dependsOn(zookeeperContainer)
                .withExternalZookeeper(getZookeeperConnection())


                //
                .withNetwork(network)
                .withNetworkAliases(KAFKA_NETWORK_ALIAS)
                .withReuse(true);

        if (StringUtils.isNotBlank(logSegmentSize)) {
            base = base.withEnv("KAFKA_LOG_SEGMENT_BYTES", logSegmentSize);
        }

        return base;
    }

    private static String getZookeeperConnection() {
//        return StringUtils.joinWith(":", zookeeperContainer.getNetworkAliases().get(0), zookeeperContainer.getFirstMappedPort());
//        return StringUtils.joinWith(":", zookeeperContainer.getHost(), zookeeperContainer.getFirstMappedPort());
//        return StringUtils.joinWith(":", zookeeperContainer.getHost(), 2181);
        return StringUtils.joinWith(":", ZOOKEEPER_NETWORK_ALIAS, ZOOKEEPER_PORT);
    }

    static {
        startKafkaContainer();
    }

    private static void startKafkaContainer() {
        log.warn("Starting broker...");
        kafkaContainer.start();
        followKafkaLogs();
        log.debug("Kafka container started");
    }


    static {
        System.setProperty("flogger.backend_factory", "com.google.common.flogger.backend.slf4j.Slf4jBackendFactory#getInstance");
    }

    private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0");

    /**
     * Toxiproxy container, which will be used as a TCP proxy
     */
    @Getter
    @Container
    private final ToxiproxyContainer toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(network)
            .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS)
            .dependsOn(kafkaContainer);

    {
        toxiproxy.start();
        if (log.isDebugEnabled()) {
            FilteredTestContainerSlf4jLogConsumer logConsumer = new FilteredTestContainerSlf4jLogConsumer(log);
            logConsumer.withPrefix("TOXIPROXY");
            toxiproxy.followOutput(logConsumer);
        }
    }

    /**
     * Starting proxying connections to a target container
     */
    @Getter
    private final ToxiproxyContainer.ContainerProxy brokerProxy = toxiproxy.getProxy(KAFKA_NETWORK_ALIAS, KAFKA_PROXY_PORT);

    {
        updateAdvertisedListenersToProxy();
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

    @Getter(AccessLevel.PROTECTED)
    private final KafkaClientUtils kcu = new KafkaClientUtils(kafkaContainer, brokerProxy);

    static void followKafkaLogs() {
        if (log.isDebugEnabled()) {
            FilteredTestContainerSlf4jLogConsumer logConsumer = new FilteredTestContainerSlf4jLogConsumer(log);
            logConsumer.withPrefix("KAFKA");
            kafkaContainer.followOutput(logConsumer);
        }
    }

    /**
     * Poor man's version of restarting the actual broker. Actually restarting the broker inside docker is difficult.
     */
    protected void restartBrokerConnectionUsingProxy() {
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

    private void updateProxyUpstream() throws IOException {
        var alias = kafkaContainer.getNetworkAliases().get(0);
        log.debug("Updating proxy upstream to point to new container: {} to new alias: {}", kafkaContainer.getContainerId(), alias);
        //        var newProxyUpstream = StringUtils.joinWith(":", kafkaContainer.getNetworkAliases().get(0), kafkaContainer.getMappedPort(KAFKA_PORT));
        var newProxyUpstream = StringUtils.joinWith(":", alias, KAFKA_PROXY_PORT);
        var proxy = getProxy();
        proxy.disable();
        proxy.setUpstream(newProxyUpstream);
        proxy.enable();
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
     * Restart the docker container such that it starts up with the same ports again
     */
    @SuppressWarnings("resource")
    protected void restartBrokerWithSamePorts() {
        kafkaContainer.setPortBindings(List.of(
                String.format("%s:%s", KAFKA_PORT, KAFKA_PORT),
                String.format("%s:%s", KAFKA_PROXY_PORT, KAFKA_PROXY_PORT),
                String.format("%s:%s", KAFKA_INTERNAL_PORT, KAFKA_INTERNAL_PORT)
        ));
    }

    @SneakyThrows
    protected void killProxyConnection() {
        log.warn("Killing first consumer connection via proxy disable"); // todo debug
        Proxy proxy = getProxy();
        proxy.disable();
        Thread.sleep(1000);
    }

    protected void reduceConnectionToZero() {
        log.debug("Reducing connection to zero");
        getBrokerProxy().setConnectionCut(true);
    }

    protected Proxy getProxy() throws IOException {
        var port = getToxiproxy().getControlPort();
        var host = getToxiproxy().getHost();
        ToxiproxyClient client = new ToxiproxyClient(host, port);
        var proxies = client.getProxies();
        var proxy = proxies.stream().findFirst().get();
        return proxy;
    }

    @SneakyThrows
    protected void restoreProxyConnection() {
        log.warn("Restore first consumer connection via proxy enable");

        Proxy proxy = getProxy();
        proxy.enable();
        Thread.sleep(1000);
    }

    protected void restoreConnectionBandwidth() {
        log.warn("Restore first consumer bandwidth via proxy");
        getBrokerProxy().setConnectionCut(false);
    }

    @BeforeEach
    void open() {
        kcu.open();
    }

    @AfterEach
    void close() {
        kcu.close();
    }

    protected String setupTopic() {
        String name = getClass().getSimpleName();
        setupTopic(name);
        return name;
    }

    protected String setupTopic(String name) {
        assertThat(kafkaContainer.isRunning()).isTrue(); // sanity

        topic = name + "-" + nextInt();

        ensureTopic(topic, numPartitions);

        return topic;
    }

    @SneakyThrows
    protected CreateTopicsResult ensureTopic(String topic, int numPartitions) {
        log.debug("Ensuring topic exists on broker: {}...", topic);
        NewTopic e1 = new NewTopic(topic, numPartitions, (short) 1);
        AdminClient admin = kcu.getAdmin();

        CreateTopicsResult topics = admin.createTopics(of(e1));
        try {
            Void all = topics.all().get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return topics;
    }

    protected List<String> produceMessages(int quantity) {
        return produceMessages(quantity, "");
    }

    @SneakyThrows
    protected List<String> produceMessages(int quantity, String prefix) {
        return getKcu().produceMessages(getTopic(), quantity, prefix);
    }

    protected void simulateBrokerResume() {
        log.warn("Simulating broker connection recovery");
        brokerProxy.setConnectionCut(false);
    }

    protected void simulateBrokerUnreachable() {
        log.warn("Simulating broker connection cut");
        brokerProxy.setConnectionCut(true);
    }

}
