package io.confluent.parallelconsumer.integrationTests;

import io.confluent.csid.testcontainers.FilteredTestContainerSlf4jLogConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nullable;

import static io.confluent.csid.utils.StringUtils.msg;
import static org.testcontainers.containers.KafkaContainer.KAFKA_PORT;

/**
 * Reusable by default, but can be made non-reusable by setting {@link #setReuse(boolean)} to false.
 *
 * @author Antony Stubbs
 */
@Slf4j
public class PCTestBroker implements Startable {

    public static final int KAFKA_INTERNAL_PORT = KAFKA_PORT - 1;

    public static final int KAFKA_PROXY_PORT = KAFKA_PORT + 1;

    private final KafkaContainer kafkaContainer = createKafkaContainer(null);

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

    protected void followContainerLogs(GenericContainer containerToFollow, String prefix) {
        FilteredTestContainerSlf4jLogConsumer logConsumer = new FilteredTestContainerSlf4jLogConsumer(log);
        logConsumer.withPrefix(prefix);
        containerToFollow.followOutput(logConsumer);
    }


    @Override
    public void start() {
        kafkaContainer.start();
        followContainerLogs(kafkaContainer, "KAFKA");
    }

    @Override
    public void stop() {
        kafkaContainer.stop();
    }
}
