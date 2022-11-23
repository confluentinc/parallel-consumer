package io.confluent.parallelconsumer.integrationTests;

import com.google.common.truth.Truth;
import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.RetrySettings.FailureReaction.RETRY_FOREVER;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.NEW_GROUP;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.ProducerMode.TRANSACTIONAL;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.shaded.org.hamcrest.Matchers.greaterThan;
import static org.testcontainers.shaded.org.hamcrest.Matchers.is;

/**
 * Exercises PC's reaction to the broker connection being lost of the broker being restarted, by stopping and starting
 * the broker container. The reaction is controlled by the
 * {@link ParallelConsumerOptions.RetrySettings.FailureReaction}
 */
@Tag("disconnect")
@Tag("toxiproxy")
@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BrokerDisconnectTest extends DedicatedBrokerIntegrationTest {

    int numberOfRecordsToProduce = 100;

    AtomicInteger processedCount = new AtomicInteger();

    ParallelEoSStreamProcessor<String, String> pc;

    @SneakyThrows
    void setupAndWarmUp(KafkaClientUtils kcu, CommitMode commitMode) {
        String topicName = setupTopic();
        kcu.produceMessages(topicName, numberOfRecordsToProduce);

        //
        var retrySettings = ParallelConsumerOptions.RetrySettings.builder()
//                .maxRetries(1)
//                .failureReaction(RETRY_UP_TO_MAX_RETRIES)
                .failureReaction(RETRY_FOREVER)
                .build();

        var preOptions = ParallelConsumerOptions.<String, String>builder()
                .ordering(UNORDERED)
                .consumer(createProxiedConsumer(getTopic()))
                .commitMode(commitMode)
                .retrySettings(retrySettings);

        if (commitMode == CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER) {
            preOptions.producer(kcu.createNewProducer(TRANSACTIONAL));
        }

        var options = preOptions.build();

        pc = kcu.buildPc(options, NEW_GROUP, 1);
        pc.subscribe(topicName);
        pc.poll(recordContexts -> {
            processedCount.incrementAndGet();
        });

        // make sure we've started consuming already before disconnecting the broker
        await().untilAtomic(processedCount, is(greaterThan(10)));
        log.debug("Consumed 100");
    }

    /**
     * Interrupts the connection to the broker by pausing the proxy and resuming, making sure PC can recover
     */
    @SneakyThrows
    @ParameterizedTest
    @EnumSource
    void brokerConnectionInterruption(CommitMode commitMode) {
        setupAndWarmUp(getKcu(), commitMode);

        //
        simulateBrokerUnreachable();

        //
        checkPCState();

        log.debug("Stay disconnected for a while...");
        Truth.assertThat(processedCount.get()).isLessThan(numberOfRecordsToProduce);
        // todo how long to test we can recover from?
        // 10 seconds, doesn't notice
        // 120 unknown error
        ThreadUtils.sleepSecondsLog(120);

        //
        checkPCState();

        //
        simulateBrokerResume();

        //
        checkPCState();

        //
        await()
                .atMost(Duration.ofMinutes(1))
                .failFast("pc has crashed", () -> pc.isClosedOrFailed())
                .untilAsserted(() -> Truth
                        .assertThat(processedCount.get())
                        .isAtLeast(numberOfRecordsToProduce));
    }

    private void checkPCState() {
        assertThat(pc).isNotClosedOrFailed();
    }

    /**
     * Stops the broker container, and waits for the broker to be unreachable. Checks that it retries for a while, then
     * gives up
     */
    @SneakyThrows
    @ParameterizedTest
    @EnumSource
    void brokerShutdown(CommitMode commitMode) {
        // need ot start
        KafkaContainer finickyKafka = createKafkaContainer(null);
        finickyKafka.start();
        BrokerIntegrationTest.followKafkaLogs(finickyKafka);

        KafkaClientUtils kafkaClientUtils = new KafkaClientUtils(finickyKafka, getBrokerProxy());

        setupAndWarmUp(kafkaClientUtils, commitMode);

        //
        brokerDies(finickyKafka);

        //
        checkPCState();

        // wait a while
        int secondsToWait = 120;
        sleepUnlessCrashed(secondsToWait);

        //
        checkPCState();

        //
        await()
                .atMost(Duration.ofMinutes(1))
                .failFast("pc has crashed", () -> pc.isClosedOrFailed())
                .untilAsserted(() -> Truth
                        .assertThat(processedCount.get())
                        .isAtLeast(numberOfRecordsToProduce));
    }

    private void sleepUnlessCrashed(int secondsToWait) {
        var start = Instant.now();
        await()
                .failFast("pc has crashed", () -> pc.isClosedOrFailed())
                .forever()
                .untilAsserted(() ->
                        assertThat(Instant.now()).isAtLeast(start.plusSeconds(secondsToWait)));
    }

    private void brokerDies(KafkaContainer finickyKafka) {
        log.error("Making broker unavailable");
        finickyKafka.close();
        getToxiproxy().close();
    }

    /**
     * Stops, then starts the broker container again. Makes sure PC can recover
     */
    @SneakyThrows
    @ParameterizedTest
    @EnumSource
    @Order(1)
    // simplest
    void brokerRestartTest(CommitMode commitMode) {
        setupAndWarmUp(getKcu(), commitMode);

        //
        restartDockerUsingCommandsAndProxy();

        //
        checkPCState();

        //
        var timeout = Duration.ofMinutes(1);
        log.debug("Waiting {} for PC to recover", timeout);
        var expectedNumberToConsumer = numberOfRecordsToProduce * 2; // record get replayed because we couldn't commit because broker "died"
        await()
                .atMost(timeout)
                .failFast("pc has crashed", () -> pc.isClosedOrFailed())
                .untilAsserted(() -> Truth
                        .assertThat(processedCount.get())
                        .isAtLeast(expectedNumberToConsumer));
    }

}
