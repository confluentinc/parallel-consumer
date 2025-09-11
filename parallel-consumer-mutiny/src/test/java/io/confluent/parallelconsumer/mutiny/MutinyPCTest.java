package io.confluent.parallelconsumer.mutiny;

import io.confluent.csid.utils.LatchTestUtils;
import io.confluent.csid.utils.ProgressBarUtils;
import io.smallrye.mutiny.Uni;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertWithMessage;
import static io.confluent.parallelconsumer.truth.LongPollingMockConsumerSubject.assertThat;
import static org.awaitility.Awaitility.await;

@Slf4j
class MutinyPCTest extends MutinyUnitTestBase {

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

        mutinyPC.onRecord(ctx -> {
            log.info("Mutiny user function: {}", ctx);
            msgs.add(ctx);
            threads.add(Thread.currentThread().getName());

            // return a Uni for async processing
            return Uni.createFrom().item(String.format("result: %d:%s", ctx.getSingleConsumerRecord().offset(), ctx.getSingleConsumerRecord().value()));
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
                            .that(threads.stream().allMatch(thread -> thread.startsWith("pc-pool")))
                            .isTrue();
                });
    }

    @Test
    void concurrencyTest() throws InterruptedException {
        int quantity = 100_000;
        var consumerRecords = ktu.generateRecords(quantity - 1); // -1 because 1 is already primed
        ktu.send(consumerSpy, consumerRecords);
        log.info("Finished priming records");

        ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, quantity);

        ConcurrentLinkedQueue<Object> msgs = new ConcurrentLinkedQueue<>();
        AtomicInteger finishedCount = new AtomicInteger(0);
        AtomicInteger maxConcurrentRecordsSeen = new AtomicInteger(0);
        CountDownLatch completeOrProblem = new CountDownLatch(1);
        int maxConcurrency = MAX_CONCURRENCY;

        mutinyPC.onRecord(ctx -> {
            var record = ctx.getSingleConsumerRecord();
            return Uni.createFrom().item(String.format("result: %d:%s", record.offset(), record.value()))
                    .onItem().invoke(ignore -> {
                        // add that our uni processing has started
                        log.trace("Mutiny user function executing: {}", ctx);
                        msgs.add(ctx);
                        if (msgs.size() > maxConcurrency) {
                            log.error("More records submitted for processing than max concurrency settings ({} vs {})", msgs.size(), maxConcurrency);
                            completeOrProblem.countDown();
                        }
                    })
                    // simulate async delay
                    .onItem().delayIt().by(Duration.ofMillis((int) (100 * Math.random())))
                    .onItem().invoke(s -> {
                        log.trace("User function after delay. Records pending: {}, removing from processing: {}", msgs.size(), ctx);
                        int currentConcurrentRecords = msgs.size();
                        int highestSoFar = Math.max(currentConcurrentRecords, maxConcurrentRecordsSeen.get());
                        maxConcurrentRecordsSeen.set(highestSoFar);

                        boolean removed = msgs.remove(ctx);
                        assertWithMessage("record was present and removed")
                                .that(removed).isTrue();

                        int numberOfFinishedRecords = finishedCount.incrementAndGet();
                        if (numberOfFinishedRecords > quantity - 1) {
                            completeOrProblem.countDown();
                        }

                        bar.step();
                    });
        });

        // block until all messages processed
        LatchTestUtils.awaitLatch(completeOrProblem, defaultTimeoutSeconds);

        int maxConcurrencyAllowedThreshold = (int) (maxConcurrency * MAX_CONCURRENCY_OVERFLOW_ALLOWANCE.value);
        assertWithMessage("Max concurrency should never be exceeded")
                .that(maxConcurrentRecordsSeen.get()).isLessThan(maxConcurrencyAllowedThreshold);

        await()
                .atMost(defaultTimeout)
                .failFast("Max concurrency exceeded", () -> msgs.size() > maxConcurrencyAllowedThreshold)
                .untilAsserted(() -> {
                    assertWithMessage("Number of completed messages")
                            .that(finishedCount.get()).isEqualTo(quantity);

                    assertThat(consumerSpy)
                            .hasCommittedToPartition(topicPartition)
                            .offset(quantity);
                });

        bar.close();
        log.info("Max concurrency was {}", maxConcurrentRecordsSeen.get());
    }
}

