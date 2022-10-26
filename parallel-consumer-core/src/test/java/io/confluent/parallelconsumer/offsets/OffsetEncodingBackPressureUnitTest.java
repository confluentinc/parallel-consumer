package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;
import io.confluent.parallelconsumer.state.PartitionState;
import io.confluent.parallelconsumer.state.PartitionStateManager;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.LongStreamEx;
import one.util.streamex.StreamEx;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.ManagedTruth.assertTruth;
import static io.confluent.parallelconsumer.ManagedTruth.assertWithMessage;
import static io.confluent.parallelconsumer.state.PartitionStateManager.USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT;

/**
 * UnitTest version of {@link OffsetEncodingBackPressureTest}.
 *
 * @see OffsetEncodingBackPressureTest
 */
@Slf4j
class OffsetEncodingBackPressureUnitTest extends ParallelEoSStreamProcessorTestBase {

    @AfterAll
    static void cleanup() {
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);
    }

    @SneakyThrows
    @Test
    @ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = ResourceAccessMode.READ_WRITE)
    void backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing() throws OffsetDecodingError {
        final int numberOfRecords = 1_00;

        // todo - very smelly - store for restoring
        var realMax = OffsetMapCodecManager.DefaultMaxMetadataSize;

        // todo don't use static public accessors to change things - makes parallel testing harder and is smelly
        OffsetMapCodecManager.DefaultMaxMetadataSize = 40; // reduce available to make testing easier
        OffsetMapCodecManager.forcedCodec = Optional.of(OffsetEncoding.BitSetV2); // force one that takes a predictable large amount of space

        //
        var wm = parallelConsumer.getWm();
        var pm = wm.getPm();
        PartitionState<String, String> partitionState = pm.getPartitionState(topicPartition);

        sendRecordsToWM(numberOfRecords, wm);

        final int numberOfBlockedMessages = 2;
        var samplingOfShouldBeCompleteOffsets = UniLists.of(1L, 50L, 99L, (long) numberOfRecords - numberOfBlockedMessages);
        var blockedOffsets = UniLists.of(0L, 2L);

        var completes = LongStreamEx.of(numberOfRecords).filter(x -> !blockedOffsets.contains(x)).boxed().toList();

        List<WorkContainer<String, String>> workIfAvailable = wm.getWorkIfAvailable();
        assertTruth(workIfAvailable).hasSize(numberOfRecords);

        List<WorkContainer<String, String>> toSucceed = workIfAvailable.stream().filter(x -> !blockedOffsets.contains(x.offset())).collect(Collectors.toList());
        toSucceed.forEach(wm::onSuccessResult);

        try {

            // # assert commit ok - nothing blocked
            {
                Optional<OffsetAndMetadata> commitDataIfDirty = partitionState.getCommitDataIfDirty();
                assertTruth(partitionState).isAllowedMoreRecords();

                int expectedHighestSeenOffset = numberOfRecords - 1;
                //         check("getOffsetHighestSucceeded()").that(actual.getOffsetHighestSucceeded()).isEqualTo(expected);
                assertTruth(partitionState).getOffsetHighestSeen().isEqualTo(expectedHighestSeenOffset);
                assertTruth(partitionState).getCommitDataIfDirty().hasOffsetEqualTo(0);
            }


            log.debug("// feed more messages in order to threshold block - as Bitset requires linearly as much space as we are feeding messages into it, it's guaranteed to block");
            int extraRecordsToBlockWithThresholdBlocks = numberOfRecords / 2;
            {
                sendRecordsToWM(extraRecordsToBlockWithThresholdBlocks, wm);
                succeedExcept(wm, blockedOffsets);

                // triggers recompute of blockage
                Optional<OffsetAndMetadata> commitDataIfDirty = partitionState.getCommitDataIfDirty();

                log.debug("// assert partition now blocked from threshold");
                assertTruth(partitionState).isNotAllowedMoreRecords();

                log.debug("// assert blocked, but can still write payload");
                assertTruth(partitionState).getCommitDataIfDirty().hasOffsetEqualTo(0L);

                // "The only incomplete record now is offset zero, which we are blocked on"
                assertTruth(partitionState).getOffsetHighestSeen().isEqualTo(numberOfRecords + extraRecordsToBlockWithThresholdBlocks - 1);
                assertTruth(partitionState).getCommitDataIfDirty().getMetadata().isNotEmpty();
                assertTruth(partitionState)
                        .getAllIncompleteOffsets()
                        .containsNoneIn(samplingOfShouldBeCompleteOffsets);
                assertWithMessage("The only incomplete record now is offset zero, which we are blocked on")
                        .that(partitionState).getAllIncompleteOffsets().containsExactlyElementsIn(blockedOffsets);
            }


            // recreates the situation where the payload size is too large and must be dropped
            log.debug("// test max payload exceeded, payload dropped");
            int processedBeforePartitionBlock = extraRecordsToBlockWithThresholdBlocks + numberOfRecords - blockedOffsets.size();
            int extraMessages = numberOfRecords + extraRecordsToBlockWithThresholdBlocks / 2;
            log.debug("// messages already sent {}, sending {} more", processedBeforePartitionBlock, extraMessages);
            {
                log.debug("// force system to allow more records (i.e. the actual system attempts to never allow the payload to grow this big)");
                PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(2);

                //
                // unlock 2L as well
                unblock(wm, workIfAvailable, 2L);
                log.debug("// unlock to make state dirty to get a commit");
                Optional<OffsetAndMetadata> commitDataIfDirty = partitionState.getCommitDataIfDirty();

                //
                log.debug("// send {} more messages", extraMessages);
                sendRecordsToWM(extraMessages, wm);
                succeedExcept(wm, UniLists.of(0L));

                log.debug("// assert payload missing from commit now");

                assertTruth(partitionState).getCommitDataIfDirty().hasOffsetEqualTo(0);
                assertTruth(partitionState).getCommitDataIfDirty().getMetadata().isEmpty();
            }

            log.debug("// test failed messages can retry");
            {
                {
                    // check it's not returned
                    List<Long> workIfAvailable1 = StreamEx.of(wm.getWorkIfAvailable()).map(WorkContainer::offset).toList();
                    assertTruth(workIfAvailable1).doesNotContain(0L);
                }

                // release message that was blocking partition progression

                wm.onFailureResult(findWC(workIfAvailable, 0L));

                {
                    List<Long> workIfAvailable1 = StreamEx.of(wm.getWorkIfAvailable()).map(WorkContainer::offset).toList();
                    assertTruth(workIfAvailable1).contains(0L);
                }


                unblock(wm, workIfAvailable, 0L);
            }

            // assert partition is now not blocked
            {

                Optional<OffsetAndMetadata> commitDataIfDirty = partitionState.getCommitDataIfDirty();
                assertTruth(partitionState).isAllowedMoreRecords();

            }

            // assert all committed, nothing blocked- next expected offset is now 1+ the offset of the final message we sent
            {

                assertTruth(partitionState).getCommitDataIfDirty().getOffset().isEqualTo(processedBeforePartitionBlock + extraMessages + numberOfBlockedMessages);

                assertTruth(partitionState).isAllowedMoreRecords();

            }
        } finally {
            // todo restore static defaults - lazy way to override settings at runtime but causes bugs by allowing them to be statically changeable
            OffsetMapCodecManager.DefaultMaxMetadataSize = realMax; // todo wow this is smelly, but convenient
            OffsetMapCodecManager.forcedCodec = Optional.empty();
        }
    }

    private void succeedExcept(WorkManager<String, String> wm, List<Long> incomplete) {
        var workIfAvailable = wm.getWorkIfAvailable();
        var toSucceed = workIfAvailable.stream()
                .filter(x -> !incomplete.contains(x.offset()))
                .collect(Collectors.toList());
        toSucceed.forEach(wm::onSuccessResult);
    }

    private void unblock(WorkManager<String, String> wm, List<WorkContainer<String, String>> from, long offsetToUnblock) {
        var unblock = findWC(from, offsetToUnblock);
        wm.onSuccessResult(unblock);
    }

    private WorkContainer<String, String> findWC(List<WorkContainer<String, String>> from, long offsetToUnblock) {
        return from.stream().filter(x -> x.offset() == offsetToUnblock).findFirst().get();
    }

    private void sendRecordsToWM(int numberOfRecords, WorkManager<String, String> wm) {
        log.debug("~Sending {} more records", numberOfRecords);
        List<ConsumerRecord<String, String>> records = ktu.generateRecords(numberOfRecords);
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(UniMaps.of(topicPartition, records)), wm.getPm()));
        Truth.assertThat(wm.getNumberOfWorkQueuedInShardsAwaitingSelection()).isEqualTo(numberOfRecords);
    }

}
