package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.github.dockerjava.api.DockerClient;
import io.confluent.csid.testcontainers.FilteredTestContainerSlf4jLogConsumer;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.testcontainers.containers.KafkaContainer.KAFKA_PORT;

/**
 * @author Antony Stubbs
 */
@Testcontainers
@Slf4j
public abstract class BrokerIntegrationTest {

    // An alias that can be used to resolve the Toxiproxy container by name in the network it is connected to.
    // It can be used as a hostname of the Toxiproxy container by other containers in the same network.
    private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";

    // Create a common docker network so that containers can communicate
//    @Rule
//    @Container
    public static Network network = Network.newNetwork();

    /**
     * https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
     * https://github.com/testcontainers/testcontainers-java/pull/1781
     */
    @Container
    @Getter(AccessLevel.PROTECTED)
    public static KafkaContainer kafkaContainer = createKafkaContainer(null);

    //advertised.listeners
    public static KafkaContainer createKafkaContainer(String logSegmentSize) {
        KafkaContainer base = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.2"))
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1") //transaction.state.log.replication.factor
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1") //transaction.state.log.min.isr
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1") //transaction.state.log.num.partitions
                //todo need to customise this for this test
                // default produce batch size is - must be at least higher than it: 16KB
                // try to speed up initial consumer group formation
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500") // group.initial.rebalance.delay.ms default: 3000
                //
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "localhost:") // for use with toxi proxy
                //
                .withNetwork(network)
                .withReuse(true);

        if (StringUtils.isNotBlank(logSegmentSize)) {
            base = base.withEnv("KAFKA_LOG_SEGMENT_BYTES", logSegmentSize);
        }

        return base;
    }

    static {
        kafkaContainer.start();
    }


    static {
        System.setProperty("flogger.backend_factory", "com.google.common.flogger.backend.slf4j.Slf4jBackendFactory#getInstance");
    }


    private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:5.0.4");
//
//    // The target container - this could be anything
////    @Rule
//    public GenericContainer<?> redis = new GenericContainer<>(REDIS_IMAGE)
//            .withExposedPorts(6379)
//            .withNetwork(network);

    private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0");

    // Toxiproxy container, which will be used as a TCP proxy
//    @Rule
    @Getter
    @Container
    private final ToxiproxyContainer toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(network)
            .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);

    {
        toxiproxy.start();
    }


    // Starting proxying connections to a target container
    @Getter
    private final ToxiproxyContainer.ContainerProxy brokerProxy = toxiproxy.getProxy(kafkaContainer, KAFKA_PORT);

    {
        updateAdvertisedListenersToProxy();
    }

    /**
     * After the proxy is started, we need to update the advertised listeners to point to the proxy, otherwise the
     * clients will try to connect directly, bypassing the proxy.
     *
     * @see KafkaContainer#containerIsStarted
     */
    @SneakyThrows
    private void updateAdvertisedListenersToProxy() {
        final KafkaClientUtils kcu = new KafkaClientUtils(kafkaContainer, null);
        var toxiPort = getBrokerProxy().getProxyPort();

        log.debug("Updating advertised listeners to point to toxi proxy: {}", toxiPort);

        //
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "1");
        // as we only have a single node cluster, only need something here which will satisfy config validation - although
        //  internally the KafkaContainer actually sets the broker to broker listener to something else
        var brokerBs = String.format("BROKER://%s:%s", kafkaContainer.getHost(), "9092");
        var bs = String.format("PLAINTEXT://%s:%s", "localhost", brokerProxy.getProxyPort());
        String value = String.join(",", brokerBs, bs);
        ConfigEntry toxiAdvertListener = new ConfigEntry("advertised.listeners", value);
        AlterConfigOp op = new AlterConfigOp(toxiAdvertListener, AlterConfigOp.OpType.SET);

        //
        // fix - reuse existing admin client? or refactor to make construction nicer
        var p = new Properties();
        p.put("bootstrap.servers", bs);
        AdminClient admin = AdminClient.create(p);
        admin.incrementalAlterConfigs(Map.of(configResource, List.of(op))).all().get();
        admin.close();

        //
        log.debug("Updated advertised listeners to point to toxi proxy: {}", toxiPort);
    }

    int numPartitions = 1;
    int partitionNumber = 0;

    @Getter
    String topic;

    @Getter(AccessLevel.PROTECTED)
    private final KafkaClientUtils kcu = new KafkaClientUtils(kafkaContainer, toxiproxy);

    @BeforeAll
    static void followKafkaLogs() {
        if (log.isDebugEnabled()) {
            FilteredTestContainerSlf4jLogConsumer logConsumer = new FilteredTestContainerSlf4jLogConsumer(log);
            kafkaContainer.followOutput(logConsumer);
        }
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

        CreateTopicsResult topics = admin.createTopics(UniLists.of(e1));
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

    int outPort = -1;

    protected void simulateBrokerResume() {
        log.warn("Simulating broker connection recovery");
        brokerProxy.setConnectionCut(false);
    }

    protected void simulateBrokerUnreachable() {
        log.warn("Simulating broker connection cut");
        brokerProxy.setConnectionCut(true);
    }

    protected void terminateBrokerWithDockerNetwork() {
        log.warn(kafkaContainer.getPortBindings().toString());
        log.warn(kafkaContainer.getExposedPorts().toString());
        log.warn(kafkaContainer.getBoundPortNumbers().toString());
        log.warn(kafkaContainer.getLivenessCheckPortNumbers().toString());

        outPort = kafkaContainer.getMappedPort(9093);

        log.debug("Test step: Terminating broker");
//        getKafkaContainer().getDockerClient()
//        String containerId = getKafkaContainer().getContainerId();
//        getKafkaContainer().getDockerClient().killContainerCmd(containerId).exec();
//        Awaitility.await().untilAsserted(() -> assertThat(kafkaContainer.isRunning()).isFalse());


        Network network = kafkaContainer.getNetwork();
        DockerClient client = kafkaContainer.getDockerClient();
        List<com.github.dockerjava.api.model.Network> exec1 = client.listNetworksCmd().exec();
//        com.github.dockerjava.api.model.Network exec = client.inspectNetworkCmd().exec();
        List<String> networkAliases = kafkaContainer.getNetworkAliases();
        String networkId = networkAliases.stream().findFirst().get();
        client.disconnectFromNetworkCmd()
                .withContainerId(kafkaContainer.getContainerId())
                .withNetworkId(networkId)
                .exec();
    }

    protected void startNewBroker() {
        log.debug("Test step: Starting a new broker");

        String mapping = "9093:" + outPort;
//        BrokerIntegrationTest.kafkaContainer = BrokerIntegrationTest.createKafkaContainer();
//        kafkaContainer.setPortBindings(UniLists.of(mapping));
//        kafkaContainer.start();

//        BrokerIntegrationTest.followKafkaLogs();
//        assertThat(kafkaContainer.isRunning()).isTrue(); // sanity
//        Awaitility.await().untilAsserted(() -> assertThat(kafkaContainer.isRunning()).isTrue());

        getKafkaContainer().getDockerClient().connectToNetworkCmd().exec();

        log.warn(kafkaContainer.getLivenessCheckPortNumbers().toString());
        log.warn(kafkaContainer.getPortBindings().toString());
        log.warn(kafkaContainer.getPortBindings().toString());

    }
}
