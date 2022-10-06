package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import one.util.streamex.LongStreamEx;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;

/**
 * @author Antony Stubbs
 * @see PartitionState#maybeTruncateBelow
 * @see PartitionState#pruneRemovedTrackedIncompleteOffsets
 */
class PartitionStateCommittedOffsetTest {

    AdminClient ac;

    ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

    TopicPartition tp = new TopicPartition("topic", 0);

    long unexpectedlyHighOffset = 20L;

    long previouslyCommittedOffset = 11L;

    final long highestSeenOffset = 101L;

    /**
     * @see PartitionState#offsetHighestSucceeded
     */
    long highestSucceeded = highestSeenOffset;

    List<Long> incompletes = UniLists.of(previouslyCommittedOffset, 15L, unexpectedlyHighOffset, 60L, 80L, 95L, 96L, 97L, 98L, 100L);

    List<Long> expectedTruncatedIncompletes = incompletes.stream()
            .filter(offset -> offset >= unexpectedlyHighOffset)
            .collect(Collectors.toList());

    HighestOffsetAndIncompletes offsetData = new HighestOffsetAndIncompletes(Optional.of(highestSeenOffset), new HashSet<>(incompletes));

    PartitionState<String, String> state = new PartitionState<>(tp, offsetData);

    /**
     * Test for offset gaps in partition data (i.e. compacted topics)
     */
    @Test
    void compactedTopic() {
        Set<Long> compacted = UniSets.of(80L, 95L, 97L);
        long slightlyLowerRange = highestSeenOffset - 2L; // to check subsets don't mess with incompletes not represented in this polled batch
        List<Long> polledOffsetsWithCompactedRemoved = LongStreamEx.range(previouslyCommittedOffset, slightlyLowerRange)
                .filter(offset -> !compacted.contains(offset))
                .boxed().toList();

        //
        PolledTestBatch polledTestBatch = new PolledTestBatch(mu, tp, polledOffsetsWithCompactedRemoved);

        //
        addPollToState(state, polledTestBatch);

        //
        OffsetAndMetadata offsetAndMetadata = state.createOffsetAndMetadata();

        assertThat(offsetAndMetadata).getOffset().isEqualTo(previouslyCommittedOffset);

        var compactedIncompletes = incompletes.stream().filter(offset -> !compacted.contains(offset)).collect(Collectors.toList());
        assertThat(state).getAllIncompleteOffsets().containsExactlyElementsIn(compactedIncompletes);

        // check still contains 100,101
    }

    /**
     * CG offset has been changed to a lower offset (partition rewind / replay).
     * <p>
     * Metadata could be lost if it's a manual reset, otherwise it will still exist. If it's been lost, then we will
     * bootstrap the partition as though it's the first time it's ever been seen, so nothing to do.
     * <p>
     * If the offset and metadata is still there, then we have to handle the situation.
     */
    @Test
    void committedOffsetLower() {
        long randomlyChosenStepBackwards = 5L;
        long unexpectedLowerOffset = previouslyCommittedOffset - randomlyChosenStepBackwards;

        PolledTestBatch polledTestBatch = new PolledTestBatch(mu, tp, unexpectedLowerOffset, highestSeenOffset);

        //
        addPollToState(state, polledTestBatch);

        //
        OffsetAndMetadata offsetAndMetadata = state.createOffsetAndMetadata();

        assertThat(offsetAndMetadata).getOffset().isEqualTo(unexpectedLowerOffset);
        assertThat(state).getAllIncompleteOffsets().containsExactlyElementsIn(LongStreamEx.range(unexpectedLowerOffset, highestSeenOffset + 1).boxed().toList());
    }

    private void addPollToState(PartitionState<String, String> state, PolledTestBatch polledTestBatch) {
        // todo when PSM and PartitionState are refactored, these two calls in PS should be a single call
        state.pruneRemovedTrackedIncompleteOffsets(polledTestBatch.polledRecordBatch.records(tp));
        for (var wc : polledTestBatch.polledBatchWCs) {
            // todo when PSM and PartitionState are refactored, this conditional should not be needed
            var offset = wc.offset();
            final boolean notPreviouslyCompleted = !state.isRecordPreviouslyCompleted(wc.getCr());
            if (notPreviouslyCompleted) {
                state.addNewIncompleteWorkContainer(wc);
            }
        }
    }
//
//    /**
//     *
//     */
//    @Test
//    void bootstrapPollOffsetHigherViaManualCGRset() {
//        // committed state
//        PartitionState<String, String> state = new PartitionState<>(tp, offsetData);
//
//        // bootstrap poll
//        PolledTestBatch polledTestBatch = new PolledTestBatch(mu, tp, unexpectedlyHighOffset, highestSeenOffset);
//
//        // todo when PSM and PartitionState are refactored, these two calls in PS should be a single call
//        addPollToState(state, polledTestBatch);
//
//        //
//        OffsetAndMetadata offsetAndMetadata = state.createOffsetAndMetadata();
//
//        assertThat(offsetAndMetadata).getOffset().isEqualTo(0L);
//        state.getAllIncompleteOffsets().containsAll(Range.range(highestSeenOffset).list());
//    }

    /**
     * CG offset has disappeared - committed offset hasn't been changed, but broker gives us a bootstrap poll result
     * with a higher offset than expected. Could be caused by retention period, or compaction.
     * <p>
     * If the CG offset has been changed to something higher than expected manually, then we will bootstrap the
     * partition as though it's never been seen before, so nothing to do.
     *
     * @implSpec issue #409: Committing old offset after OFFSET_OUT_OF_RANGE
     * @see PartitionState#maybeTruncateBelow
     */
    @Test
    void bootstrapPollOffsetHigherDueToRetentionOrCompaction() {
        // committed state
        PartitionState<String, String> state = new PartitionState<>(tp, offsetData);

        // bootstrap poll
        PolledTestBatch polledTestBatch = new PolledTestBatch(mu, tp, unexpectedlyHighOffset, highestSeenOffset);

        // todo when PSM and PartitionState are refactored, these two calls in PS should be a single call
        addPollToState(state, polledTestBatch);

        //
        assertThat(state).getNextExpectedInitialPolledOffset().isEqualTo(unexpectedlyHighOffset);
        OffsetAndMetadata offsetAndMetadata = state.createOffsetAndMetadata();

        assertThat(offsetAndMetadata).getOffset().isEqualTo(unexpectedlyHighOffset);
        assertThat(state).getAllIncompleteOffsets().containsExactlyElementsIn(expectedTruncatedIncompletes);
    }


}