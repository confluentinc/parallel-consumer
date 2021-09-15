package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.FakeRuntimeError;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.JavaUtils.getLast;
import static io.confluent.csid.utils.JavaUtils.getOnlyOne;
import static io.confluent.csid.utils.LatchTestUtils.awaitLatch;
import static io.confluent.csid.utils.ThreadUtils.sleepQuietly;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * Writes to static state to perform test - needs to be run in isolation. However it runs very fast, so it doesn't slow
 * down parallel test suite much.
 * <p>
 * Runs in isolation regardless of the resource lock read/write setting, because actually various tests depend
 * indirectly on the behaviour of the metadata size, even if not so explicitly.
 * <p>
 * See {@link OffsetMapCodecManager#METADATA_DATA_SIZE_RESOURCE_LOCK}
 */
@Isolated // messes with static state - breaks other tests running in parallel
@Slf4j
class OffsetEncodingBackPressureTest extends ParallelEoSStreamProcessorTestBase {

    /**
     * Tests that when required space for encoding offset becomes too large, back pressure is put into the system so
     * that no further messages for the given partitions can be taken for processing, until more messages complete.
     */
    @Test
    // needed due to static accessors in parallel tests
    @ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = ResourceAccessMode.READ_WRITE)
    void backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing() throws OffsetDecodingError {
        // mock messages downloaded for processing > MAX_TO_QUEUE
        // make sure work manager doesn't queue more than MAX_TO_QUEUE
//        final int numRecords = 1_000_0;
        final int numRecords = 1_00;
        parallelConsumer.setLongPollTimeout(ofMillis(200));

        // todo - very smelly - store for restoring
        var realMax = OffsetMapCodecManager.DefaultMaxMetadataSize;

        // todo don't use static public accessors to change things - makes parallel testing harder and is smelly
        OffsetMapCodecManager.DefaultMaxMetadataSize = 40; // reduce available to make testing easier
        OffsetMapCodecManager.forcedCodec = Optional.of(OffsetEncoding.BitSetV2); // force one that takes a predictable large amount of space

        ktu.send(consumerSpy, ktu.generateRecords(numRecords));

        AtomicInteger processedCount = new AtomicInteger(0);
        CountDownLatch msgLock = new CountDownLatch(1);
        CountDownLatch msgLockTwo = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger(0);
        long offsetToBlock = 0;

        parallelConsumer.poll((rec) -> {

            // block the partition to create bigger and bigger offset encoding blocks
            // don't let offset 0 finish
            if (rec.offset() == offsetToBlock) {
                int attemptNumber = attempts.incrementAndGet();
                if (attemptNumber == 1) {
                    log.debug("force first message to 'never' complete, causing a large offset encoding (lots of messages completing above the low water mark");
                    awaitLatch(msgLock, 120);
                    log.debug("very slow message awoken, throwing exception");
                    throw new FakeRuntimeError("Fake error");
                } else {
                    log.debug("Second attempt, sleeping");
                    awaitLatch(msgLockTwo, 60);
                    log.debug("Second attempt, unlocked, succeeding");
                }
            } else {
                sleepQuietly(1);
            }
            processedCount.getAndIncrement();
        });

        try {

            // wait for all pre-produced messages to be processed and produced
            waitAtMost(ofSeconds(120))
                    // dynamic reason support still waiting https://github.com/awaitility/awaitility/pull/193#issuecomment-873116199
                    .failFast("PC died - check logs", parallelConsumer::isClosedOrFailed)
                    //, () -> parallelConsumer.getFailureCause()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
                    .pollInterval(1, SECONDS)
                    .untilAsserted(() -> {
                        assertThat(processedCount.get()).isEqualTo(99L);
                    });

            // assert commit ok - nothing blocked
            {
                //
                waitForSomeLoopCycles(1);
                parallelConsumer.requestCommitAsap();
                waitForSomeLoopCycles(1);

                //
                List<OffsetAndMetadata> offsetAndMetadataList = extractAllPartitionsOffsetsAndMetadataSequentially();
                assertThat(offsetAndMetadataList).isNotEmpty();
                OffsetAndMetadata offsetAndMetadata = offsetAndMetadataList.get(offsetAndMetadataList.size() - 1);
                assertThat(offsetAndMetadata.offset()).isZero();

                //
                String metadata = offsetAndMetadata.metadata();
                HighestOffsetAndIncompletes decodedOffsetPayload = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(offsetToBlock, metadata);
                Long highestSeenOffset = decodedOffsetPayload.getHighestSeenOffset();
                Set<Long> incompletes = decodedOffsetPayload.getIncompleteOffsets();
                assertThat(incompletes).isNotEmpty()
                        .contains(offsetToBlock)
                        .doesNotContain(1L, 50L, 99L, (long) numRecords - 1); // some sampling of completed offsets, 99 being the highest
                assertThat(highestSeenOffset).isEqualTo(99L);
            }

            WorkManager<String, String> wm = parallelConsumer.getWm();

            // partition not blocked
            {
                boolean partitionBlocked = wm.getPm().isBlocked(topicPartition);
                assertThat(partitionBlocked).isFalse();
            }

            // feed more messages in order to threshold block - as Bitset requires linearly as much space as we are feeding messages into it, it's gauranteed to block
            int extraRecordsToBlockWithThresholdBlocks = numRecords / 2;
            {
                assertThat(wm.getPm().isAllowedMoreRecords(topicPartition)).isTrue(); // should initially be not blocked

                ktu.send(consumerSpy, ktu.generateRecords(extraRecordsToBlockWithThresholdBlocks));
                waitForOneLoopCycle();

                // assert partition now blocked from threshold
                waitAtMost(ofSeconds(30)).untilAsserted(() -> assertThat(wm.getPm().isBlocked(topicPartition))
                        .as("Partition SHOULD be blocked due to back pressure")
                        .isTrue()); // blocked

                Long partitionOffsetHighWaterMarks = wm.getPm().getHighestSeenOffset(topicPartition);
                assertThat(partitionOffsetHighWaterMarks)
                        .isGreaterThan(numRecords); // high watermark is beyond our initial processed count upon blocking

                parallelConsumer.requestCommitAsap();
                waitForOneLoopCycle();

                // assert blocked, but can still write payload
                // assert the committed offset metadata contains a payload
                waitAtMost(ofSeconds(120)).untilAsserted(() ->
                        {
                            OffsetAndMetadata partitionCommit = getLastCommit();
                            //
                            assertThat(partitionCommit.offset()).isZero();
                            //
                            String meta = partitionCommit.metadata();
                            HighestOffsetAndIncompletes incompletes = OffsetMapCodecManager
                                    .deserialiseIncompleteOffsetMapFromBase64(0L, meta);
                            Truth.assertWithMessage("The only incomplete record now is offset zero, which we are blocked on")
                                    .that(incompletes.getIncompleteOffsets()).containsExactly(0L);
                            assertThat(incompletes.getHighestSeenOffset()).isEqualTo(processedCount.get());
                        }
                );
            }

            // test max payload exceeded, payload dropped
            int processedBeforePartitionBlock = processedCount.get();
            int extraMessages = numRecords + extraRecordsToBlockWithThresholdBlocks / 2;
            {
                // force system to allow more records (i.e. the actual system attempts to never allow the payload to grow this big)
                wm.getPm().setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(2);

                //
                ktu.send(consumerSpy, ktu.generateRecords(extraMessages));

                //
                await().atMost(ofSeconds(60)).untilAsserted(() ->
                        assertThat(processedCount.get()).isEqualTo(processedBeforePartitionBlock + extraMessages) // some new message processed
                );
                // assert payload missing from commit now
                await().untilAsserted(() -> {
                    OffsetAndMetadata partitionCommit = getLastCommit();
                    assertThat(partitionCommit.offset()).isZero();
                    assertThat(partitionCommit.metadata()).isBlank(); // missing offset encoding as too large
                });
            }

            // test failed messages can retry
            {
                Duration aggressiveDelay = ofMillis(100);
                WorkContainer.setDefaultRetryDelay(aggressiveDelay); // more aggressive retry

                // release message that was blocking partition progression
                // fail the message
                msgLock.countDown();

                // wait for the retry
                waitForOneLoopCycle();
                sleepQuietly(aggressiveDelay.toMillis());
                await().until(() -> attempts.get() >= 2);

                // assert partition still blocked
                waitForOneLoopCycle();
                await().untilAsserted(() -> assertThat(wm.getPm().isAllowedMoreRecords(topicPartition)).isFalse());

                // release the message for the second time, allowing it to succeed
                msgLockTwo.countDown();
            }

            // assert partition is now not blocked
            {
                waitForOneLoopCycle();
                await().untilAsserted(() -> assertThat(wm.getPm().isAllowedMoreRecords(topicPartition)).isTrue());
            }

            // assert all committed, nothing blocked- next expected offset is now 1+ the offset of the final message we sent
            {
                await().untilAsserted(() -> {
                    List<Integer> offsets = extractAllPartitionsOffsetsSequentially();
                    assertThat(offsets).contains(processedCount.get());
                });
                await().untilAsserted(() -> assertThat(wm.getPm().isAllowedMoreRecords(topicPartition)).isTrue());
            }
        } finally {
            // make sure to unlock threads - speeds up failed tests, instead of waiting for latch or close timeouts
            msgLock.countDown();
            msgLockTwo.countDown();

            // todo restore static defaults - lazy way to override settings at runtime but causes bugs by allowing them to be statically changeable
            OffsetMapCodecManager.DefaultMaxMetadataSize = realMax; // todo wow this is smelly, but convenient
            OffsetMapCodecManager.forcedCodec = Optional.empty();
        }


    }

    private OffsetAndMetadata getLastCommit() {
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> commitHistory = getCommitHistory();
        Map<String, Map<TopicPartition, OffsetAndMetadata>> lastCommit = getLast(commitHistory).get();
        Map<TopicPartition, OffsetAndMetadata> allPartitionCommits = getOnlyOne(lastCommit).get();
        return allPartitionCommits.get(topicPartition);
    }

}
