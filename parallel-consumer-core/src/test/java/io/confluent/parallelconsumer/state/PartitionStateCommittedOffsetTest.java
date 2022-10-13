package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import io.confluent.parallelconsumer.offsets.OffsetEncodingTests;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import one.util.streamex.LongStreamEx;
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
 * Unit test for PartitionState behaviour when committed offsets are changed and random records are removed (compaction)
 * which already are tracked in the offset map.
 *
 * @author Antony Stubbs
 * @see OffsetEncodingTests#ensureEncodingGracefullyWorksWhenOffsetsAreVeryLargeAndNotSequential
 * @see PartitionState#maybeTruncateBelow
 * @see PartitionState#maybeTruncateOrPruneTrackedOffsets
 * @see io.confluent.parallelconsumer.integrationTests.state.PartitionStateCommittedOffsetIT
 */
class PartitionStateCommittedOffsetTest {

    ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

    TopicPartition tp = new TopicPartition("topic", 0);

    long unexpectedlyHighOffset = 20L;

    long previouslyCommittedOffset = 11L;

    final long highestSeenOffset = 101L;

    List<Long> incompletes = UniLists.of(previouslyCommittedOffset, 15L, unexpectedlyHighOffset, 60L, 80L, 95L, 96L, 97L, 98L, 100L);

    List<Long> expectedTruncatedIncompletes = incompletes.stream()
            .filter(offset -> offset >= unexpectedlyHighOffset)
            .collect(Collectors.toList());

    HighestOffsetAndIncompletes offsetData = new HighestOffsetAndIncompletes(Optional.of(highestSeenOffset), new HashSet<>(incompletes));

    PartitionState<String, String> state = new PartitionState<>(0, mu.getModule(), tp, offsetData);

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
        state.maybeRegisterNewPollBatchAsWork(polledTestBatch.polledRecordBatch.records(state.getTopicPartition()));
    }

    /**
     * CG offset has disappeared - committed offset hasn't been changed, but broker gives us a bootstrap poll result
     * with a higher offset than expected. Could be caused by retention period, or compaction.
     * <p>
     * If the CG offset has been changed to something higher than expected manually, then we will bootstrap the
     * partition as though it's never been seen before, so nothing to do.
     *
     * @implSpec issue #409: Committing old offset after OFFSET_OUT_OF_RANGE
     * @see PartitionState#maybeTruncateBelow
     * @see OffsetEncodingTests#ensureEncodingGracefullyWorksWhenOffsetsAreVeryLargeAndNotSequential
     */
    @Test
    void bootstrapPollOffsetHigherDueToRetentionOrCompaction() {
        // bootstrap poll
        PolledTestBatch polledTestBatch = new PolledTestBatch(mu, tp, unexpectedlyHighOffset, highestSeenOffset);

        //
        addPollToState(state, polledTestBatch);

        //
        Truth.assertThat(state.getNextExpectedInitialPolledOffset()).isEqualTo(unexpectedlyHighOffset);
        OffsetAndMetadata offsetAndMetadata = state.createOffsetAndMetadata();

        assertThat(offsetAndMetadata).getOffset().isEqualTo(unexpectedlyHighOffset);
        assertThat(state).getAllIncompleteOffsets().containsExactlyElementsIn(expectedTruncatedIncompletes);
    }


}