package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

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
import static io.confluent.csid.utils.StringUtils.msg;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.testcontainers.containers.KafkaContainer.KAFKA_PORT;

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

    int numPartitions = 1;

    int partitionNumber = 0;

    @Getter
    String topic;

    /**
     * Create a common docker network so that containers can communicate with each other
     */
    public static Network network = Network.newNetwork();

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
            .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);

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
    private final ToxiproxyContainer.ContainerProxy brokerProxy = toxiproxy.getProxy(kafkaContainer, KAFKA_PROXY_PORT);

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
        final KafkaClientUtils kcu = new KafkaClientUtils(kafkaContainer, null); // todo fix smell
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

    @BeforeAll
    static void followKafkaLogs() {
        if (log.isDebugEnabled()) {
            FilteredTestContainerSlf4jLogConsumer logConsumer = new FilteredTestContainerSlf4jLogConsumer(log);
            logConsumer.withPrefix("KAFKA");
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

    protected void simulateBrokerResume() {
        log.warn("Simulating broker connection recovery");
        brokerProxy.setConnectionCut(false);
    }

    protected void simulateBrokerUnreachable() {
        log.warn("Simulating broker connection cut");
        brokerProxy.setConnectionCut(true);
    }

}
