package io.confluent.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.testcontainers.FilteredTestContainerSlf4jLogConsumer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nullable;
import java.util.Properties;

import static io.confluent.csid.utils.StringUtils.msg;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.testcontainers.containers.KafkaContainer.KAFKA_PORT;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Our wrapper for {@link KafkaContainer}, which is a TestContainer for Kafka.
 * <p>
 * Reusable by default, but can be made non-reusable by setting {@link KafkaContainer#withReuse} to false.
 *
 * @author Antony Stubbs
 */
@Slf4j
public class PCTestBroker implements Startable {

    public static final int KAFKA_INTERNAL_PORT = KAFKA_PORT - 1;

    public static final int KAFKA_PROXY_PORT = KAFKA_PORT + 1;

    public static final String CONTAINER_PREFIX = "csid-pc-";

    @Getter(AccessLevel.PROTECTED)
    protected final KafkaContainer kafkaContainer;

    private KafkaClientUtils kcu;

    public PCTestBroker() {
        this(null);
    }

    public PCTestBroker(@Nullable String logSegmentSize) {
        kafkaContainer = createKafkaContainer(logSegmentSize);
        kcu = new KafkaClientUtils(this);
    }

    /**
     * @param logSegmentSize if null, will use default
     * @see #updateAdvertisedListenersToProxy()
     */
    public KafkaContainer createKafkaContainer(@Nullable String logSegmentSize) {
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

                .withReuse(true);

        if (StringUtils.isNotBlank(logSegmentSize)) {
            base = base.withEnv("KAFKA_LOG_SEGMENT_BYTES", logSegmentSize);
        }

        return base;
    }

    protected AdminClient createDirectAdminClient() {
        var p = new Properties();
        // Kafka Container overrides our env with it's configure method, so have to do some gymnastics
        var boostrapForAdmin = String.format("PLAINTEXT://%s:%s", "localhost", getKafkaContainer().getMappedPort(KAFKA_PORT));
        p.put(BOOTSTRAP_SERVERS_CONFIG, boostrapForAdmin);
        p.put(CommonClientConfigs.CLIENT_ID_CONFIG, getClass().getSimpleName() + "-admin-" + System.currentTimeMillis());
        AdminClient admin = AdminClient.create(p);
        return admin;
    }

    protected void followContainerLogs(GenericContainer<?> containerToFollow, String prefix) {
        FilteredTestContainerSlf4jLogConsumer logConsumer = new FilteredTestContainerSlf4jLogConsumer(log);
        logConsumer.withPrefix(prefix);
        containerToFollow.followOutput(logConsumer);
    }

    @Override
    public void start() {
        log.debug("Broker starting...");
        kafkaContainer.start();
        followContainerLogs(kafkaContainer, "KAFKA");
        injectContainerPrefix(kafkaContainer);
        kcu.open();
        log.debug("Broker started {}", getDirectBootstrapServers());
    }

    @Override
    public void stop() {
        kcu.close();
//        kcu = null; // todo remove - was test to see if admin accessed after close
        kafkaContainer.stop();
    }

    protected void injectContainerPrefix(GenericContainer<?> container) {
        var dockerClient = DockerClientFactory.lazyClient();

        // check prefix not already injected - for reused containers
        var containerInfo = dockerClient.inspectContainerCmd(container.getContainerId()).exec();
        if (StringUtils.startsWith(containerInfo.getName(), "/" + CONTAINER_PREFIX)) {
            log.debug("Container already has prefix, skipping");
            return;
        }

        // inject prefix
        var name = getContainerPrefix() + StringUtils.removeStart(container.getContainerName(), "/");
        dockerClient.renameContainerCmd(container.getContainerId())
                .withName(name)
                .exec();
    }

    protected String getContainerPrefix() {
        return CONTAINER_PREFIX + "reuse-";
    }

    @SneakyThrows
    public CreateTopicsResult ensureTopic(String topic, int numPartitions) {
        log.debug("Ensuring topic exists on broker: {}...", topic);
        NewTopic e1 = new NewTopic(topic, numPartitions, (short) 1);

        var admin = getAdmin();

        CreateTopicsResult topics = admin.createTopics(of(e1));
        try {
            Void all = topics.all().get(2, MINUTES);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return topics;
    }

    protected AdminClient getAdmin() {
        return getKcu().getAdmin();
    }

    public boolean isRunning() {
        return kafkaContainer.isRunning();
    }

    protected KafkaClientUtils getKcu() {
        return kcu;
    }

    public KafkaClientUtils createKcu() {
        var kafkaClientUtils = new KafkaClientUtils(this);
        kafkaClientUtils.open();
        return kafkaClientUtils;
    }

    public String getDirectBootstrapServers() {
        var bootstraps = String.format("PLAINTEXT://%s:%s", getDirectHost(), kafkaContainer.getMappedPort(KAFKA_PORT));
        return bootstraps;
    }

    private String getDirectHost() {
        return kafkaContainer.getHost();
    }

    public String getKafkaContainerId() {
        return kafkaContainer.getContainerId();
    }
}
