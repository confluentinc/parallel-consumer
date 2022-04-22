package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import com.google.common.truth.Truth8;
import io.confluent.parallelconsumer.FakeRuntimeError;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import io.confluent.parallelconsumer.controller.*;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.JavaUtils.getLast;
import static io.confluent.csid.utils.JavaUtils.getOnlyOne;
import static io.confluent.csid.utils.LatchTestUtils.awaitLatch;
import static io.confluent.csid.utils.ThreadUtils.sleepQuietly;
import static io.confluent.parallelconsumer.ManagedTruth.assertWithMessage;
import static io.confluent.parallelconsumer.controller.PartitionStateManager.USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT;
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
 *
 * @see OffsetMapCodecManager#METADATA_DATA_SIZE_RESOURCE_LOCK
 * @see OffsetEncodingBackPressureUnitTest
 */
@Isolated // messes with static state - breaks other tests running in parallel
@Slf4j
class OffsetEncodingBackPressureTest extends ParallelEoSStreamProcessorTestBase {

    @AfterAll
    static void cleanup() {
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);
    }

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
        final int numberOfRecords = 1_00;
        parallelConsumer.setTimeBetweenCommits(ofSeconds(1));

        // todo - very smelly - store for restoring
        var realMax = OffsetMapCodecManager.DefaultMaxMetadataSize;

        // todo don't use static public accessors to change things - makes parallel testing harder and is smelly
        OffsetMapCodecManager.DefaultMaxMetadataSize = 40; // reduce available to make testing easier
        OffsetMapCodecManager.forcedCodec = Optional.of(OffsetEncoding.BitSetV2); // force one that takes a predictable large amount of space

        //
        List<ConsumerRecord<String, String>> records = ktu.generateRecords(numberOfRecords);
        ktu.send(consumerSpy, records);

        AtomicInteger userFuncFinishedCount = new AtomicInteger();
        AtomicInteger userFuncStartCount = new AtomicInteger();

        CountDownLatch msgLock = new CountDownLatch(1);
        CountDownLatch msgLockTwo = new CountDownLatch(1);
        CountDownLatch msgLockThree = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger(0);
        long offsetToBlock = 0;
        List<Long> blockedOffsets = UniLists.of(0L, 2L);
        final int numberOfBlockedMessages = blockedOffsets.size();

        WorkManager<String, String> wm = parallelConsumer.getWm();
        final PartitionState<String, String> partitionState = wm.getPm().getPartitionState(topicPartition);

        ConcurrentLinkedQueue<Long> seen = new ConcurrentLinkedQueue<>();

        parallelConsumer.poll((rec) -> {
            seen.add(rec.offset());
            userFuncStartCount.incrementAndGet();
            // block the partition to create bigger and bigger offset encoding blocks
            // don't let offset 0 finish
            if (rec.offset() == offsetToBlock) {
                int attemptNumber = attempts.incrementAndGet();
                if (attemptNumber == 1) {
                    log.debug("Force first message to 'never' complete, causing a large offset encoding (lots of messages completing above the low water mark. Waiting for msgLock countdown.");
                    int timeout = 120;
                    awaitLatch(msgLock, timeout);
                    log.debug("Very slow message awoken, throwing exception");
                    throw new FakeRuntimeError("Fake error");
                } else {
                    log.debug("Second attempt, waiting for msgLockTwo countdown");
                    awaitLatch(msgLockTwo, 60);
                    log.debug("Second attempt, unlocked, succeeding");
                }
            } else if (rec.offset() == 2L) {
                awaitLatch(msgLockThree);
                log.debug("// msg 2L unblocked");
            } else {
                sleepQuietly(1);
            }
            userFuncFinishedCount.incrementAndGet();
        });

        ShardManager<String, String> sm = wm.getSm();

        try {

            // wait for all pre-produced messages to be processed and produced
            waitAtMost(ofSeconds(120))
                    // dynamic reason support still waiting https://github.com/awaitility/awaitility/pull/193#issuecomment-873116199
                    .failFast("PC died - check logs", parallelConsumer::isClosedOrFailed)
                    //, () -> parallelConsumer.getFailureCause()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
                    .pollInterval(1, SECONDS)
                    .untilAsserted(() -> {
                        assertThat(userFuncFinishedCount.get()).isEqualTo(numberOfRecords - numberOfBlockedMessages);
                    });

            // # assert commit ok - nothing blocked
            {
                //
                awaitForSomeLoopCycles(1);
                parallelConsumer.requestCommitAsap();
                awaitForSomeLoopCycles(1);

                // initial 0 offset is committed with they offset encoded payload
                assertThatConsumer("Initial commit has been executed")
                        .hasCommittedToAnyPartition()
                        .offset(0);
                List<OffsetAndMetadata> offsetAndMetadataList = extractAllPartitionsOffsetsAndMetadataSequentially();
                OffsetAndMetadata mostRecentCommit = getLast(offsetAndMetadataList).get();
                assertThat(mostRecentCommit.offset()).isZero();

                // check offset encoding incomplete payload doesn't contain expected completed messages
                String metadata = mostRecentCommit.metadata();
                HighestOffsetAndIncompletes decodedOffsetPayload = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(0, metadata);
                Long highestSeenOffset = decodedOffsetPayload.getHighestSeenOffset().get();
                Set<Long> incompletes = decodedOffsetPayload.getIncompleteOffsets();
                assertThat(incompletes).isNotEmpty()
                        .contains(offsetToBlock)
                        .doesNotContain(1L, 50L, 99L, (long) numberOfRecords - numberOfBlockedMessages); // some sampling of completed offsets, 99 being the highest
                int expectedHighestSeenOffset = numberOfRecords - 1;
                assertThat(highestSeenOffset).as("offset 99 is encoded as having been seen").isEqualTo(expectedHighestSeenOffset);
            }


            // partition not blocked
            {
                boolean partitionBlocked = wm.getPm().isBlocked(topicPartition);
                assertThat(partitionBlocked).isFalse();
            }

            //
            log.debug("// feed more messages in order to threshold block - as Bitset requires linearly as much space as we are feeding messages into it, it's guaranteed to block");
            int extraRecordsToBlockWithThresholdBlocks = numberOfRecords / 2;
            {
                assertThat(wm.getPm().isAllowedMoreRecords(topicPartition)).isTrue(); // should initially be not blocked

                ktu.send(consumerSpy, ktu.generateRecords(extraRecordsToBlockWithThresholdBlocks));
                awaitForOneLoopCycle();

                log.debug("// assert partition now blocked from threshold");
                waitAtMost(ofSeconds(10))
                        .untilAsserted(
                                () -> assertWithMessage("Partition SHOULD be blocked due to back pressure")
                                        .that(partitionState)
                                        .isBlocked()); // blocked

                Long partitionOffsetHighWaterMarks = wm.getPm().getHighestSeenOffset(topicPartition);
                assertThat(partitionOffsetHighWaterMarks)
                        .isGreaterThan(numberOfRecords); // high watermark is beyond our initial processed count upon blocking

                parallelConsumer.requestCommitAsap();
                awaitForOneLoopCycle();

                log.debug("// assert blocked, but can still write payload");
                // assert the committed offset metadata contains a payload
                waitAtMost(defaultTimeout).untilAsserted(() ->
                        {
                            OffsetAndMetadata partitionCommit = getLastCommit();
                            //
                            assertThat(partitionCommit.offset()).isZero();
                            //
                            String meta = partitionCommit.metadata();
                            HighestOffsetAndIncompletes incompletes = OffsetMapCodecManager
                                    .deserialiseIncompleteOffsetMapFromBase64(0L, meta);
                            Truth.assertWithMessage("The only incomplete record now is offset zero, which we are blocked on")
                                    .that(incompletes.getIncompleteOffsets()).containsExactlyElementsIn(blockedOffsets);
                            int expectedHighestSeen = numberOfRecords + extraRecordsToBlockWithThresholdBlocks - 1;
                            Truth8.assertThat(incompletes.getHighestSeenOffset()).hasValue(expectedHighestSeen);
                        }
                );
            }

            // recreates the situation where the payload size is too large and must be dropped
            log.debug("// test max payload exceeded, payload dropped");
            {
                log.debug("// force system to allow more records (i.e. the actual system attempts to never allow the payload to grow this big)");
                PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(30);
                parallelConsumer.requestCommitAsap();
                awaitForOneLoopCycle();


                //
                log.debug("// unlock to make state dirty to get a commit");

                msgLockThree.countDown();
                int processedBeforePartitionBlock = userFuncFinishedCount.get();
                int extraMessages = numberOfRecords + extraRecordsToBlockWithThresholdBlocks / 2;
                log.debug("// send {} more messages", extraMessages);
                ktu.send(consumerSpy, ktu.generateRecords(extraMessages));

                awaitForOneLoopCycle();
                parallelConsumer.requestCommitAsap();

                log.debug("// wait for the new message to be processed");
                await().atMost(defaultTimeout).untilAsserted(() ->
                        {
                            int expectedUserFunctionFinishedCount = processedBeforePartitionBlock + extraMessages + 1;
                            assertThat(userFuncFinishedCount.get()).isEqualTo(expectedUserFunctionFinishedCount);
                        }
                );

                log.debug("// assert payload missing from commit now");
                await().untilAsserted(() -> {
                    OffsetAndMetadata partitionCommit = getLastCommit();
                    assertThat(partitionCommit.offset()).isZero();
                    assertThat(partitionCommit.metadata()).isBlank(); // missing offset encoding as too large
                });
            }

            log.debug("// test failed messages can retry");
            {
                Duration aggressiveDelay = ofMillis(100);
                WorkContainer.setDefaultRetryDelay(aggressiveDelay); // more aggressive retry

                // release message that was blocking partition progression
                // fail the message
                msgLock.countDown();

                // wait for the retry
                awaitForOneLoopCycle();
                sleepQuietly(aggressiveDelay.toMillis());
                await().until(() -> attempts.get() >= 2);

                // assert partition still blocked
                awaitForOneLoopCycle();
                await().untilAsserted(() -> assertThat(wm.getPm().isAllowedMoreRecords(topicPartition)).isFalse());

                // release the message for the second time, allowing it to succeed
                msgLockTwo.countDown();
            }

            // assert partition is now not blocked
            {
                awaitForOneLoopCycle();
                await().untilAsserted(() -> assertThat(wm.getPm().isAllowedMoreRecords(topicPartition)).isTrue());
            }

            // assert all committed, nothing blocked- next expected offset is now 1+ the offset of the final message we sent
            {
                await().untilAsserted(() -> {
                    List<Integer> offsets = extractAllPartitionsOffsetsSequentially(false);
                    assertThat(offsets).contains(userFuncFinishedCount.get());
                });
                await().untilAsserted(() -> assertThat(wm.getPm().isAllowedMoreRecords(topicPartition)).isTrue());
            }
        } finally {
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
