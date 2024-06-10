package io.confluent.parallelconsumer.reactor;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.LatchTestUtils;
import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.csid.utils.StringUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertWithMessage;
import static io.confluent.parallelconsumer.truth.LongPollingMockConsumerSubject.assertThat;
import static org.awaitility.Awaitility.await;

@Slf4j
class ReactorPCTest extends ReactorUnitTestBase {

    /**
     * The percent of the max concurrency tolerance allowed
     */
    public static final Percentage MAX_CONCURRENCY_OVERFLOW_ALLOWANCE = Percentage.withPercentage(1.2);

    @BeforeEach
    public void setupData() {
        super.primeFirstRecord();
    }

    @Test
    void kickTires() {
        primeFirstRecord();
        primeFirstRecord();
        primeFirstRecord();

        ConcurrentLinkedQueue<Object> msgs = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> threads = new ConcurrentLinkedQueue<>();

        reactorPC.react((rec) -> {
            log.info("Reactor user poll function: {}", rec);
            msgs.add(rec);
            threads.add(Thread.currentThread().getName());
            return Mono.just(StringUtils.msg("result: {}:{}", rec.offset(), rec.value()));
        });

        await()
                .atMost(defaultTimeout)
                .untilAsserted(() -> {
                    assertWithMessage("Processed records collection so far")
                            .that(msgs.size())
                            .isEqualTo(4);

                    assertThat(consumerSpy)
                            .hasCommittedToPartition(topicPartition)
                            .atLeastOffset(4);

                    assertWithMessage("The user-defined function should be executed by the scheduler")
                            .that(threads.stream().allMatch(thread -> thread.startsWith("boundedElastic")))
                            .isTrue();
                });
    }

    @SneakyThrows
    @Test
    void concurrencyTest() {
        //
        var quantity = 100_000;
        var consumerRecords = ktu.generateRecords(quantity - 1); // -1 coz already has 1 record primed (all tests do)
        ktu.send(consumerSpy, consumerRecords);
        log.info("Finished priming records");

        //
        ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, quantity);

        //
        ConcurrentLinkedQueue<Object> msgs = new ConcurrentLinkedQueue<>();

        var finishedCount = new AtomicInteger(0);
        var maxConcurrentRecordsSeen = new AtomicInteger(0);
        var completeOrProblem = new CountDownLatch(1);
        var maxConcurrency = MAX_CONCURRENCY;

        reactorPC.react(recordContext -> Mono.just(StringUtils.msg("result: {}:{}", recordContext.offset(), recordContext.value()))
                .doOnNext(ignore -> {
                    // add that our mono processing has started
                    log.trace("Reactor user function executing: {}", recordContext);
                    msgs.add(recordContext);
                    if (msgs.size() > maxConcurrency) {
                        log.error("More records submitted for processing than max concurrency settings ({} vs {})", msgs.size(), maxConcurrency);
                        // fail fast - test already failed
                        completeOrProblem.countDown();
                    }
                })
                // delay the Mono to simulate a slow async processing time, to cause our concurrency to be reached for sure
                .delayElement(Duration.ofMillis((int) (100 * Math.random())))
                .doOnNext(s -> {
                    log.trace("User function after delay. Records pending: {}, removing from out for processing: {}", msgs.size(), recordContext);
                    int currentConcurrentRecords = msgs.size();
                    int highestSoFar = Math.max(currentConcurrentRecords, maxConcurrentRecordsSeen.get());
                    maxConcurrentRecordsSeen.set(highestSoFar);

                    //
                    boolean removed = msgs.remove(recordContext);
                    assertWithMessage("record was present and removed")
                            .that(removed).isTrue();

                    //
                    int numberOfFinishedRecords = finishedCount.incrementAndGet();
                    boolean allExpectedRecordsAreProcessed = numberOfFinishedRecords > quantity - 1;
                    if (allExpectedRecordsAreProcessed) {
                        // release the latch to indicate processing complete
                        completeOrProblem.countDown();
                    }

                    //
                    bar.step();
                }));

        // block here until all messages processed
        LatchTestUtils.awaitLatch(completeOrProblem, defaultTimeoutSeconds);

        //
        int maxConcurrencyAllowedThreshold = (int) (maxConcurrency * MAX_CONCURRENCY_OVERFLOW_ALLOWANCE.value);
        assertWithMessage("Max concurrency should never be exceeded")
                .that(maxConcurrentRecordsSeen.get()).isLessThan(maxConcurrencyAllowedThreshold);
        log.info("Max concurrency was {}", maxConcurrentRecordsSeen.get());

        //
        await()
                // perform testing for at least some time - see fail fast
                .atMost(defaultTimeout)
                // make sure out for processing recs never exceeds max concurrency
                .failFast("Max concurrency exceeded", () -> msgs.size() > maxConcurrencyAllowedThreshold)
                .untilAsserted(() -> {
                    assertWithMessage("Number of completed messages")
                            .that(finishedCount.get()).isEqualTo(quantity);

                    assertThat(consumerSpy).hasCommittedToPartition(topicPartition).offset(quantity);
                });

        bar.close();
        log.info("Max concurrency was {}", maxConcurrentRecordsSeen.get());
    }
}
