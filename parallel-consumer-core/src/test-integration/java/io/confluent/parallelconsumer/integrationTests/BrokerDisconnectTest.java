package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.ManagedTruth;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.RetrySettings;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.RetrySettings.FailureReaction.RETRY_FOREVER;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.RetrySettings.FailureReaction.RETRY_UP_TO_MAX_RETRIES;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.NEW_GROUP;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.shaded.org.hamcrest.Matchers.greaterThan;
import static org.testcontainers.shaded.org.hamcrest.Matchers.is;

/**
 * Exercises PC's reaction to the broker connection being lost of the broker being restarted, by stopping and starting
 * the broker container. The reaction is controlled by the {@link RetrySettings.FailureReaction}
 */
@Tag("disconnect")
@Tag("toxiproxy")
@Slf4j
@Timeout(180)
class BrokerDisconnectTest extends DedicatedBrokerIntegrationTest {

    int numberOfRecordsToProduce = 1000;

    AtomicInteger processedCount = new AtomicInteger();

    ParallelEoSStreamProcessor<String, String> pc;

    String groupId;

    String topicName;


    /**
     * Retry forever by default
     */
    private void setupAndWarmUp(CommitMode commitMode) {
        setupAndWarmUp(commitMode, RetrySettings.builder().failureReaction(RETRY_FOREVER).build());
    }

    @SneakyThrows
    void setupAndWarmUp(CommitMode commitMode, RetrySettings retrySettings) {
        topicName = setupTopic();
        groupId = getTopic();
        getKcu().produceMessages(topicName, numberOfRecordsToProduce);

        var preOptions = ParallelConsumerOptions.<String, String>builder()
                .ordering(UNORDERED)
                .consumer(getChaosBroker().createProxiedConsumer(groupId))
                .commitMode(commitMode)
                .retrySettings(retrySettings);

        if (commitMode == CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER) {
            var prod = getChaosBroker().createProxiedTransactionalProducer();
            preOptions.producer(prod);
        }

        var options = preOptions.build();

        pc = getKcu().buildPc(options, NEW_GROUP, 1);
        pc.subscribe(topicName);
        pc.poll(recordContexts -> {
            var count = processedCount.incrementAndGet();
            log.debug("Processed: offset: {} count: {}", recordContexts.offset(), count);
        });

        // make sure we've started consuming already before disconnecting the broker
        await().untilAtomic(processedCount, is(greaterThan(10)));
        log.debug("Consumed {}", processedCount.get());
    }

    /**
     * Interrupts the connection to the broker by pausing the proxy and resuming, making sure PC can recover
     */
    @SneakyThrows
    @ParameterizedTest
    @EnumSource
    void brokerConnectionInterruption(CommitMode commitMode) {
        setupAndWarmUp(commitMode);

        //
        getChaosBroker().simulateBrokerUnreachable();

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
        getChaosBroker().simulateBrokerReachable();

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
        var giveUpAfter3 = RetrySettings.builder()
                .maxRetries(3)
                .failureReaction(RETRY_UP_TO_MAX_RETRIES)
                .build();

        setupAndWarmUp(commitMode, giveUpAfter3);

        //
        getChaosBroker().stop();

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

    /**
     * Stops, then starts the broker container again. Makes sure PC can recover
     */
    @SneakyThrows
    @ParameterizedTest
    @EnumSource
    @Order(1)
    // simplest
    void brokerRestartTest(CommitMode commitMode) {
        setupAndWarmUp(commitMode);

        //
        log.debug("Restarting broker - consumed so far: {}", processedCount.get());

        getChaosBroker().sendStop();

        // sleep long enough that the commit timeout triggers
        ThreadUtils.sleepLog(70);

        //
        getChaosBroker().sendStart();

        //
        pc.requestCommitAsap();

        //
        checkPCState();

        //
        var timeout = Duration.ofMinutes(1);
        log.debug("Waiting {} for PC to recover", timeout);

        // will be exact if no replay, otherwise will be more
        // as pc is not crashing - it doesn't lose any state - i.e. per rec acks
        // we're just testing that it can recover from a broker restart
        var expectedNumberToConsumer = numberOfRecordsToProduce;

        await()
                .atMost(timeout)
                .failFast("pc has crashed", () -> pc.isClosedOrFailed())
                .untilAsserted(() -> Truth
                        .assertThat(processedCount.get())
                        .isAtLeast(expectedNumberToConsumer));

        pc.closeDrainFirst();

        try (var proxiedConsumer = getChaosBroker().createProxiedConsumer(groupId)) {
            ManagedTruth.assertThat(proxiedConsumer)
                    .hasCommittedToPartition(topicName)
                    .offset(expectedNumberToConsumer);
        }
    }

}
