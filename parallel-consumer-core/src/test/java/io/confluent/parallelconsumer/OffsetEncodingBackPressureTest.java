package io.confluent.parallelconsumer;

import io.confluent.csid.utils.TrimListRepresentation;
import io.confluent.parallelconsumer.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

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

@Slf4j
public class OffsetEncodingBackPressureTest extends ParallelEoSStreamProcessorTestBase {

    /**
     * Tests that when required space for encoding offset becomes too large, back pressure is put into the system so
     * that no further messages for the given partitions can be taken for processing, until more messages complete.
     */
    @SneakyThrows
    @Test
    void backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing() {
        // mock messages downloaded for processing > MAX_TO_QUEUE
        // make sure work manager doesn't queue more than MAX_TO_QUEUE
//        final int numRecords = 1_000_0;
        final int numRecords = 1_00;
        parallelConsumer.setLongPollTimeout(ofMillis(200));
//        parallelConsumer.setTimeBetweenCommits();

        var realMax = OffsetMapCodecManager.DefaultMaxMetadataSize;

        OffsetMapCodecManager.DefaultMaxMetadataSize = 40; // reduce available to make testing easier
        OffsetMapCodecManager.forcedCodec = Optional.of(OffsetEncoding.BitSetV2); // force one that takes a predictable large amount of space

        ktu.send(consumerSpy, ktu.generateRecords(numRecords));

        AtomicInteger processedCount = new AtomicInteger(0);
        CountDownLatch msgLock = new CountDownLatch(1);
        CountDownLatch msgLockTwo = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger(0);
        parallelConsumer.poll((rec) -> {

            // block the partition to create bigger and bigger offset encoding blocks
            if (rec.offset() == 0) {
                int attemptNumber = attempts.incrementAndGet();
                if (attemptNumber == 1) {
                    log.debug("force first message to 'never' complete, causing a large offset encoding (lots of messages completing above the low water mark");
                    awaitLatch(msgLock, 60);
                    log.debug("very slow message awoken, throwing exception");
                    throw new RuntimeException("Fake error");
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

        // wait for all pre-produced messages to be processed and produced
        Assertions.useRepresentation(new TrimListRepresentation());
        waitAtMost(ofSeconds(120))
//                .failFast(() -> parallelConsumer.isClosedOrFailed(), () -> parallelConsumer.getFailureCause()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
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
            assertThat(offsetAndMetadata.offset()).isEqualTo(0L);

            //
            String metadata = offsetAndMetadata.metadata();
            HighestOffsetAndIncompletes decodedOffsetPayload = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(0, metadata);
            Long highestSeenOffset = decodedOffsetPayload.getHighestSeenOffset();
            Set<Long> incompletes = decodedOffsetPayload.getIncompleteOffsets();
            assertThat(incompletes).isNotEmpty().contains(0L).doesNotContain(1L, 50L, 99L);
            assertThat(highestSeenOffset).isEqualTo(99L);
        }

        WorkManager<String, String> wm = parallelConsumer.wm;

        // partition not blocked
        {
            boolean partitionBlocked = !wm.partitionMoreRecordsAllowedToProcess.get(topicPartition);
            assertThat(partitionBlocked).isFalse();
        }

        // feed more messages in order to threshold block
        int extraRecordsToBlockWithThresholdBlocks = numRecords / 2;
        int expectedMsgsProcessedUponThresholdBlock = numRecords + (extraRecordsToBlockWithThresholdBlocks / 2);
        {
            ktu.send(consumerSpy, ktu.generateRecords(extraRecordsToBlockWithThresholdBlocks));
            waitForOneLoopCycle();
            assertThat(wm.partitionMoreRecordsAllowedToProcess.get(topicPartition)).isTrue(); // should initially be not blocked
            await().untilAsserted(() ->
                    assertThat(processedCount.get()).isGreaterThan(expectedMsgsProcessedUponThresholdBlock) // some new message processed
            );
            waitForOneLoopCycle();
        }

        // assert partition now blocked from threshold
        int expectedMsgsProcessedBeforePartitionBlocks = numRecords + numRecords / 4;
        {
            assertThat(wm.partitionOffsetHighWaterMarks.get(topicPartition))
                    .isGreaterThan(expectedMsgsProcessedBeforePartitionBlocks); // high water mark is beyond our expected processed count upon blocking
            assertThat(wm.partitionMoreRecordsAllowedToProcess.get(topicPartition)).isFalse(); // blocked

            // assert blocked, but can still write payload
            // assert the committed offset metadata contains a payload
            await().untilAsserted(() ->
                    {
                        OffsetAndMetadata partitionCommit = getLastCommit();
                        //
                        assertThat(partitionCommit.offset()).isEqualTo(0);
                        //
                        String meta = partitionCommit.metadata();
                        HighestOffsetAndIncompletes incompletes = OffsetMapCodecManager
                                .deserialiseIncompleteOffsetMapFromBase64(0L, meta);
                        assertThat(incompletes.getIncompleteOffsets()).containsOnly(0L);
                        assertThat(incompletes.getHighestSeenOffset()).isEqualTo(processedCount.get());
                    }
            );
        }

        // test max payload exceeded, payload dropped
        {
            // force system to allow more records (i.e. the actual system attempts to never allow the payload to grow this big)
            WorkManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(2); // set system threshold > 100% to allow too many messages to process so we high our hard limit

            //
            ktu.send(consumerSpy, ktu.generateRecords(expectedMsgsProcessedBeforePartitionBlocks));

            //
            await().atMost(ofSeconds(5)).untilAsserted(() ->
                    assertThat(processedCount.get()).isGreaterThan(expectedMsgsProcessedBeforePartitionBlocks) // some new message processed
            );
            // assert payload missing from commit now
            await().untilAsserted(() -> {
                OffsetAndMetadata partitionCommit = getLastCommit();
                assertThat(partitionCommit.offset()).isEqualTo(0);
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
            await().untilAsserted(() -> assertThat(wm.partitionMoreRecordsAllowedToProcess.get(topicPartition)).isFalse());

            // release the message for the second time, allowing it to succeed
            msgLockTwo.countDown();
        }

        // assert partition is now not blocked
        {
            waitForOneLoopCycle();
            await().untilAsserted(() -> assertThat(wm.partitionMoreRecordsAllowedToProcess.get(topicPartition)).isTrue());
        }

        // assert all committed, nothing blocked- next expected offset is now 1+ the offset of the final message we sent (numRecords*2)
        {
            int nextExpectedOffsetAfterSubmittedWork = numRecords * 2;
            await().untilAsserted(() -> {
                List<Integer> offsets = extractAllPartitionsOffsetsSequentially();
                assertThat(offsets).contains(processedCount.get());
            });
            await().untilAsserted(() -> assertThat(wm.partitionMoreRecordsAllowedToProcess.get(topicPartition)).isTrue());
        }

        OffsetMapCodecManager.DefaultMaxMetadataSize = realMax; // todo wow this is smelly, but convenient
    }

    private OffsetAndMetadata getLastCommit() {
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> commitHistory = getCommitHistory();
        Map<String, Map<TopicPartition, OffsetAndMetadata>> lastCommit = getLast(commitHistory).get();
        Map<TopicPartition, OffsetAndMetadata> allPartitionCommits = getOnlyOne(lastCommit).get();
        return allPartitionCommits.get(topicPartition);
    }

}
