package io.confluent.parallelconsumer;

import io.confluent.csid.utils.TrimListRepresentation;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.ThreadUtils.sleepQueietly;
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

        OffsetMapCodecManager.DefaultMaxMetadataSize = 40;
        OffsetMapCodecManager.forcedCodec = Optional.of(OffsetEncoding.BitSetV2); // force one that takes a predictable large amount of space

        //
//        int maxConcurrency = 200;
//        ParallelConsumerOptions<String, String> build = ParallelConsumerOptions.<String, String>builder()
//                .commitMode(TRANSACTIONAL_PRODUCER)
//                .maxConcurrency(maxConcurrency)
//                .build();
//        WorkManager<String, String> wm = new WorkManager<>(build, consumerManager);

        ktu.send(consumerSpy, ktu.generateRecords(numRecords));

        AtomicInteger processedCount = new AtomicInteger(0);
        CountDownLatch msgLock = new CountDownLatch(1);
        parallelConsumer.poll((rec) -> {
            // block the partition to create bigger and bigger offset encoding blocks
            if (rec.offset() == 0) {
                log.debug("force first message to 'never' complete, causing a large offset encoding (lots of messages completing above the low water mark");
                awaitLatch(msgLock, 60);
                log.debug("very slow message awoken");
            } else {
                sleepQueietly(5);
            }
            processedCount.getAndIncrement();
        });
//
//        // add records
//        {
//            ConsumerRecords<String, String> crs = buildConsumerRecords(numRecords);
//            wm.registerWork(crs);
//        }

        // wait for all pre-produced messages to be processed and produced
        Assertions.useRepresentation(new TrimListRepresentation());
//        var failureMessage = StringUtils.msg("All keys sent to input-topic should be processed and produced, within time (expected: {} commit: {} order: {} max poll: {})",
//                expectedMessageCount, commitMode, order, maxPoll);
//        try {
        waitAtMost(ofSeconds(1200))
                .failFast(() -> parallelConsumer.isClosedOrFailed(), () -> parallelConsumer.getFailureCause()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
//                    .alias(failureMessage)
                .pollInterval(1, SECONDS)
                .untilAsserted(() -> {
//                        log.trace("Processed-count: {}, Produced-count: {}", processedCount.get(), producedCount.get());
//                        SoftAssertions all = new SoftAssertions();
//                        all.assertThat(new ArrayList<>(consumedKeys)).as("all expected are consumed").hasSameSizeAs(expectedKeys);
//                        all.assertThat(new ArrayList<>(producedKeysAcknowledged)).as("all consumed are produced ok ").hasSameSizeAs(expectedKeys);
//                        all.assertAll();
                    assertThat(processedCount.get()).isEqualTo(numRecords - 1);
                });
//        } catch (ConditionTimeoutException e) {
//            fail(failureMessage + "\n" + e.getMessage());
//        }

        // assert commit ok
        {
            waitForSomeLoopCycles(1);
            parallelConsumer.requestCommitAsap();
            waitForSomeLoopCycles(1);
            List<OffsetAndMetadata> offsetAndMetadataList = extractAllPartitionsOffsetsAndMetadataSequentially();
            assertThat(offsetAndMetadataList).isNotEmpty();
            OffsetAndMetadata offsetAndMetadata = offsetAndMetadataList.get(offsetAndMetadataList.size() - 1);
            assertThat(offsetAndMetadata.offset()).isEqualTo(0L);
            String metadata = offsetAndMetadata.metadata();
            OffsetMapCodecManager.NextOffsetAndIncompletes longTreeSetTuple = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(0, metadata);
            // todo naming here?
            Long highestSucceeded = longTreeSetTuple.getNextExpectedOffset();
            assertThat(highestSucceeded).isEqualTo(99L);
            Set<Long> incompletes = longTreeSetTuple.getIncompleteOffsets();
            assertThat(incompletes).isNotEmpty().contains(0L).doesNotContain(1L, 50L, 99L);
        }

        //
        WorkManager<String, String> wm = parallelConsumer.wm;
        Boolean partitionBlocked = !wm.partitionMoreRecordsAllowedToProcess.get(topicPartition);
        assertThat(partitionBlocked).isTrue();

        // feed more messages
        ktu.send(consumerSpy, ktu.generateRecords(numRecords));

        // assert partition blocked
        waitForOneLoopCycle();
        assertThat(wm.partitionMoreRecordsAllowedToProcess.get(topicPartition)).isFalse();

        // release message that was blocking partition progression
        msgLock.countDown();

        // assert no partitions blocked
        waitForOneLoopCycle();
        await().untilAsserted(() -> assertThat(wm.partitionMoreRecordsAllowedToProcess.get(topicPartition)).isTrue());

        // assert all committed
        int nextExpectedOffsetAfterSubmittedWork = numRecords * 2;
        await().untilAsserted(() -> assertThat(extractAllPartitionsOffsetsSequentially()).contains(nextExpectedOffsetAfterSubmittedWork));
        await().untilAsserted(() -> assertThat(wm.partitionMoreRecordsAllowedToProcess.get(topicPartition)).isTrue());
    }

    @Test
    void failedMessagesThatCanRetryDontDeadlockABlockedPartition() {
    }

}
