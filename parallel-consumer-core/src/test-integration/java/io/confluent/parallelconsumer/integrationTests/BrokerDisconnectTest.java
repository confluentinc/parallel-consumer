package io.confluent.parallelconsumer.integrationTests;

import com.google.common.truth.Truth;
import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.RetrySettings.FailureReaction.RETRY_UP_TO_MAX_RETRIES;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.NEW_GROUP;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.shaded.org.hamcrest.Matchers.greaterThan;
import static org.testcontainers.shaded.org.hamcrest.Matchers.is;

/**
 * Exercises PC's reaction to the broker connection being lost of the broker being restarted
 */
@Tag("disconnect")
@Tag("toxiproxy")
@Slf4j
class BrokerDisconnectTest extends BrokerIntegrationTest {

    int recordsProduced = 100;

    AtomicInteger processedCount = new AtomicInteger();

    ParallelEoSStreamProcessor<String, String> pc;

    @SneakyThrows
    void setupAndWarmUp(KafkaClientUtils kcu, CommitMode commitMode) {
        String topicName = setupTopic();
        kcu.produceMessages(topicName, recordsProduced);

        //
        var retrySettings = ParallelConsumerOptions.RetrySettings.builder()
                .maxRetries(1)
                .failureReaction(RETRY_UP_TO_MAX_RETRIES)
                .build();

        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(UNORDERED)
                .commitMode(commitMode)
                .retrySettings(retrySettings)
//                .producer(kcu.getProducer())
                .build();

        pc = kcu.buildPc(options, NEW_GROUP, 1);
        pc.subscribe(topicName);
        pc.poll(recordContexts -> {
            processedCount.incrementAndGet();
        });

        // make sure we've started consuming already before disconnecting the broker
        await().untilAtomic(processedCount, is(greaterThan(10)));
        log.debug("Consumed 100");
    }

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
        Truth.assertThat(processedCount.get()).isLessThan(recordsProduced);
        // todo how long to test we can recover from?
        // 10 seconds, doesn't notice
        // 120 unknown error
        ThreadUtils.sleepSecondsLog(180);

        //
        checkPCState();

        //
        simulateBrokerResume();

        //
        checkPCState();

        //
        await()
                .atMost(Duration.ofMinutes(60))
                .failFast("pc has crashed", () -> pc.isClosedOrFailed())
                .untilAsserted(() -> Truth
                        .assertThat(processedCount.get())
                        .isAtLeast(recordsProduced));
    }

    private void checkPCState() {
        assertThat(pc).isNotClosedOrFailed();
    }

    @SneakyThrows
    @ParameterizedTest
    @EnumSource
    void brokerShutdown(CommitMode commitMode) {
        // need ot start
        KafkaContainer finickyKafka = getKafkaContainer();

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
                .atMost(Duration.ofMinutes(60))
                .failFast("pc has crashed", () -> pc.isClosedOrFailed())
                .untilAsserted(() -> Truth
                        .assertThat(processedCount.get())
                        .isAtLeast(recordsProduced));
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

    @SneakyThrows
    @ParameterizedTest
    @EnumSource
    void brokerRestartTest(CommitMode commitMode) {
        setupAndWarmUp(getKcu(), commitMode);

        //
        restartBroker();

        //
        checkPCState();

        log.debug("Stay disconnected for a while...");
        Truth.assertThat(processedCount.get()).isLessThan(recordsProduced);
        // todo how long to test we can recover from?
        // 10 seconds, doesn't notice
        // 120 unknown error
        ThreadUtils.sleepSecondsLog(180);

        //
        checkPCState();

        //
        await()
                .atMost(Duration.ofMinutes(60))
                .failFast("pc has crashed", () -> pc.isClosedOrFailed())
                .untilAsserted(() -> Truth
                        .assertThat(processedCount.get())
                        .isAtLeast(recordsProduced));
    }

    private void restartBroker() {
        log.error("Closing broker...");
        kafkaContainer.stop();
        log.error("Starting broker...");
        kafkaContainer.start();
    }

}
