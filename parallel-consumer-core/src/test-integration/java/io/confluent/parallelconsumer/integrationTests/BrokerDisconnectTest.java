package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.ParallelConsumerException;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.RetrySettings;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.RetrySettings.FailureReaction.RETRY_FOREVER;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.RetrySettings.FailureReaction.RETRY_UP_TO_MAX_RETRIES;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.NEW_GROUP;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.ProducerMode.TRANSACTIONAL;
import static io.confluent.parallelconsumer.internal.ConsumerManager.DEFAULT_API_TIMEOUT;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.shaded.org.hamcrest.Matchers.greaterThan;
import static org.testcontainers.shaded.org.hamcrest.Matchers.is;

/**
 * Exercises PC's reaction to the broker connection being lost of the broker being restarted, by stopping and starting
 * the broker container. The reaction is controlled by the {@link RetrySettings.FailureReaction}.
 */
@Tag("disconnect")
@Tag("toxiproxy")
@Slf4j
@Timeout(180)
class BrokerDisconnectTest extends DedicatedBrokerIntegrationTest {

    public static final Duration TEST_COMMIT_TIMEOUT = Duration.ofSeconds(10);
    int numberOfRecordsToProduce = 1000;

    AtomicInteger processedCount = new AtomicInteger();

    ParallelEoSStreamProcessor<String, String> pc;

    String groupId;

    String topicName;

    private Duration processDelay;

    /**
     * Test is designed to use a slow consume rate, so that the broker can be stopped and started before the consumer
     * finished consuming everything, or buffers too quickly.
     */
    @Override
    protected Optional<Integer> withFetchMaxBytes() {
        return Optional.of(1);
    }

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
                .offsetCommitTimeout(TEST_COMMIT_TIMEOUT) // aggressive to speed up tests
                .consumer(getKcu().createNewConsumer(groupId))
                .commitMode(commitMode)
                .retrySettings(retrySettings);

        if (commitMode == CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER) {
            var prod = getKcu().<String, String>createNewProducer(TRANSACTIONAL);
            preOptions.producer(prod);
        }

        var options = preOptions.build();

        pc = getKcu().buildPc(options, NEW_GROUP, 1);
        pc.subscribe(topicName);
        pc.poll(recordContexts -> {
            var count = processedCount.incrementAndGet();
            log.debug("Processed: offset: {} count: {}", recordContexts.offset(), count);
            ThreadUtils.sleepLog(processDelay); // slow down processing, causing state to continue to be dirty
        });

        // make sure we've started consuming already before disconnecting the broker
        await().untilAtomic(processedCount, is(greaterThan(10)));
        log.debug("Consumed {}", processedCount.get());
    }

    /**
     * Interrupts the connection to the broker by pausing the proxy and resuming, making sure PC can recover. Unless
     * using transaction mode, in which case the PC will crash as Producer needs to be reinitialised if a timeout occurs
     * with the broker.
     */
    @SneakyThrows
    @ParameterizedTest
    @EnumSource
    void brokerConnectionInterruption(CommitMode commitMode) {
        processDelay = Duration.ofMillis(10);

        setupAndWarmUp(commitMode);

        //
        getChaosBroker().simulateBrokerUnreachable();

        //
        checkPCState();

        log.debug("Stay disconnected for a while...");
        Truth.assertThat(processedCount.get()).isLessThan(numberOfRecordsToProduce);

        /**
         * producer transactions use {@link io.confluent.parallelconsumer.internal.ConsumerManager#DEFAULT_API_TIMEOUT},
         * so have to more than that, unless we can change it
         * */
        ThreadUtils.sleepSecondsLog(120);

        //
        checkPCState();

        //
        getChaosBroker().simulateBrokerReachable();

        //
        checkPCState();

        switch (commitMode) {
            case PERIODIC_TRANSACTIONAL_PRODUCER -> {
                await()
                        .atMost(Duration.ofMinutes(1))
                        .untilAsserted(() -> {
                            assertThat(pc).isClosedOrFailed();
                            var throwableSubject = assertThat(pc).getFailureCause();
                            throwableSubject.hasMessageThat().contains("Timeout expired");
                            throwableSubject.hasMessageThat().contains("while awaiting AddOffsetsToTxn");
                        });
            }
            default -> {
                // all commit modes should recover
                await()
                        .atMost(Duration.ofMinutes(1))
                        .failFast("pc has crashed", () -> pc.isClosedOrFailed())
                        .untilAsserted(() -> Truth
                                .assertThat(processedCount.get())
                                .isAtLeast(numberOfRecordsToProduce));
            }
        }

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
        processDelay = Duration.ofMillis(100);

        var giveUpAfter3 = RetrySettings.builder()
                .maxRetries(3)
                .failureReaction(RETRY_UP_TO_MAX_RETRIES)
                .build();

        setupAndWarmUp(commitMode, giveUpAfter3);

        //
        getChaosBroker().stop();

        //
        checkPCState();

        // should crash itself after giving up offset commit
        await()
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() ->
                        {
                            var failureCause = assertThat(pc).getFailureCause();
                            failureCause.isNotNull();
                            failureCause.isInstanceOf(ParallelConsumerException.class);
                            switch (commitMode) {
                                case PERIODIC_TRANSACTIONAL_PRODUCER -> {
                                    failureCause.hasMessageThat().contains("while awaiting AddOffsetsToTxn");
                                }
                                case PERIODIC_CONSUMER_SYNC -> {
                                    failureCause.hasMessageThat().contains("Timeout committing offsets");
                                }
                                case PERIODIC_CONSUMER_ASYNCHRONOUS -> {
                                    failureCause.hasCauseThat().hasCauseThat().hasMessageThat().contains("Retries exhausted");
                                }
                            }
                        }
                );
    }

    /**
     * Stops, then starts the broker container again. Makes sure PC can recover, unless using transactional mode, in
     * which case PC will crash as the Transactional Producer needs to be reinitialised across a broker restart.
     * <p>
     * {@link CommitMode#PERIODIC_CONSUMER_ASYNCHRONOUS} version of this test might sensitive to timing, as we have to
     * get it right, so it continues trying to commit as state becomes dirty, long enough so that it lasts over the
     * restart sleep.
     */
    @SneakyThrows
    @ParameterizedTest
    @EnumSource
    void brokerRestartTest(CommitMode commitMode) {
        processDelay = Duration.ofMillis(1000);

        setupAndWarmUp(commitMode);

        //
        log.debug("Restarting broker - consumed so far: {}", processedCount.get());

        getChaosBroker().sendStop();

        // sleep long enough that the commit timeout triggers
        var duration = switch (commitMode) {
            // TX producer can't have its timeouts easily changed
            case PERIODIC_TRANSACTIONAL_PRODUCER -> DEFAULT_API_TIMEOUT.plusSeconds(10);
            default -> TEST_COMMIT_TIMEOUT.multipliedBy(3);
        };
        ThreadUtils.sleepLog(duration);

        //
        getChaosBroker().sendStart();

        //
        pc.requestCommitAsap();

        //
        checkPCState();

        //
        var timeout = Duration.ofMinutes(1);
        log.debug("Waiting {} for PC to recover", timeout);

        switch (commitMode) {
            case PERIODIC_TRANSACTIONAL_PRODUCER -> {
                // should not recover, as a tx can't span a restarted broker
                await()
                        .atMost(Duration.ofMinutes(2))
                        .untilAsserted(() ->
                                {
                                    var failureCause = assertThat(pc).getFailureCause();
                                    failureCause.isNotNull();
                                    failureCause.isInstanceOf(ParallelConsumerException.class);
                                    failureCause.hasMessageThat().contains("Timeout expired");
                                    failureCause.hasMessageThat().contains("awaiting AddOffsetsToTxn");
                                }
                        );
            }
            default -> {

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

                try (var proxiedConsumer = getKcu().createNewConsumer(groupId)) {
                    assertThat(proxiedConsumer)
                            .hasCommittedToPartition(topicName)
                            .offset(expectedNumberToConsumer);
                }
            }
        }

    }

}
