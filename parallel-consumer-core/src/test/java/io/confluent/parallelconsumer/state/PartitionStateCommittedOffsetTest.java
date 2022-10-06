package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.csid.utils.Range;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;

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

    /**
     * @see PartitionState#maybeTruncateBelow
     */
    // todo parameter test with offsets closer together to check off by one
    @Test
    void bootstrapPollOffsetHigherDueToRentention() {
        PartitionState<String, String> state = new PartitionState<>(tp, offsetData);

        // bootstrap the first record, triggering truncation - it's offset #unexpectedlyHighOffset, but we were expecting #previouslyCommittedOffset
        addPollToState(state, new PolledTestBatch(mu, tp, unexpectedlyHighOffset, highestSeenOffset));

        //
        Truth.assertThat(state.getNextExpectedInitialPolledOffset()).isEqualTo(unexpectedlyHighOffset);

        Truth.assertThat(state.getIncompleteOffsetsBelowHighestSucceeded()).containsExactlyElementsIn(expectedTruncatedIncompletes);

    }

    /**
     * Test for offset gaps in partition data (i.e. compacted topics)
     */
    @Test
    void compactedTopic() {

    }

    /**
     * CG offset has been changed to a lower offset (partition rewind / replay) (metdata lost?)
     */
    @Test
    void committedOffsetLower() {
        PartitionState<String, String> state = new PartitionState<>(tp, offsetData);

        long unexpectedLowerOffset = previouslyCommittedOffset - 5L;

        PolledTestBatch polledTestBatch = new PolledTestBatch(mu, tp, unexpectedLowerOffset, highestSeenOffset);

        //
//        var psm = new PartitionStateManager<String, String>(mu.getModule(), mock(ShardManager.class));
//        psm.onass
//        psm.maybeRegisterNewRecordAsWork(polledTestBatch.polledRecordBatch);

        addPollToState(state, polledTestBatch);

        //
        OffsetAndMetadata offsetAndMetadata = state.createOffsetAndMetadata();

        assertThat(offsetAndMetadata).getOffset().isEqualTo(0L);
        state.getAllIncompleteOffsets().containsAll(Range.range(highestSeenOffset).list());
    }

    private void addPollToState(PartitionState<String, String> state, PolledTestBatch polledTestBatch) {
        // todo when PSM and PartitionState are refactored, these two calls in PS should be a single call
        state.pruneRemovedTrackedIncompleteOffsets(polledTestBatch.polledRecordBatch.records(tp));
        for (var wc : polledTestBatch.polledBatchWCs) {
            // todo when PSM and PartitionState are refactored, this conditional should not be needed
            if (!state.isRecordPreviouslyCompleted(wc.getCr())) {
                state.addNewIncompleteWorkContainer(wc);
            }
        }
    }

    /**
     * CG offset has been changed to something higher than expected (offset skip) (metadata lost?)
     */
    @Test
    void bootstrapPollOffsetHigherViaManualCGRset() {
        // committed state
        PartitionState<String, String> state = new PartitionState<>(tp, offsetData);

        // bootstrap poll
        PolledTestBatch polledTestBatch = new PolledTestBatch(mu, tp, unexpectedlyHighOffset, highestSeenOffset);

        // todo when PSM and PartitionState are refactored, these two calls in PS should be a single call
        addPollToState(state, polledTestBatch);

        //
        OffsetAndMetadata offsetAndMetadata = state.createOffsetAndMetadata();

        assertThat(offsetAndMetadata).getOffset().isEqualTo(0L);
        state.getAllIncompleteOffsets().containsAll(Range.range(highestSeenOffset).list());
    }

    /**
     * CG offset has disappeared - committed offset hasn't been changed, but broker gives us a bootstrap poll result
     * with a higher offset than expected. Could be caused by retention period, or compaction.
     *
     * @implSpec issue #409: Committing old offset after OFFSET_OUT_OF_RANGE
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