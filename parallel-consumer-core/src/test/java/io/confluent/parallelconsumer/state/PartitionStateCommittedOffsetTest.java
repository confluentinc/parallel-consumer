package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.csid.utils.Range;
import io.confluent.parallelconsumer.ManagedTruth;
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

class PartitionStateCommittedOffsetTest {

    AdminClient ac;

    ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

    TopicPartition tp = new TopicPartition("topic", 0);

    long unexpectedlyHighOffset = 20L;

    final long previouslyCommittedOffset = 11L;

    List<Long> incompletes = UniLists.of(previouslyCommittedOffset, 15L, unexpectedlyHighOffset, 60L, 80L);

    List<Long> expectedTruncatedIncompletes = incompletes.stream()
            .filter(offset -> offset >= unexpectedlyHighOffset)
            .collect(Collectors.toList());

    final long highestSeenOffset = 100L;

    HighestOffsetAndIncompletes offsetData = new HighestOffsetAndIncompletes(Optional.of(highestSeenOffset), new HashSet<>(incompletes));

    /**
     * @see PartitionState#maybeTruncateBelow
     */
    // todo parameter test with offsets closer together to check off by one
    @Test
    void bootstrapTruncation() {
        PartitionState<String, String> state = new PartitionState<>(tp, offsetData);

        var w20 = mu.createWorkFor(unexpectedlyHighOffset);

        // bootstrap the first record, triggering truncation - it's offset #unexpectedlyHighOffset, but we were expecting #previouslyCommittedOffset
        state.addNewIncompleteWorkContainer(w20);


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

        PolledTestBatch polledTestBatch = new PolledTestBatch(mu, tp, highestSeenOffset);

        //
//        var psm = new PartitionStateManager<String, String>(mu.getModule(), mock(ShardManager.class));
//        psm.onass
//        psm.maybeRegisterNewRecordAsWork(polledTestBatch.polledRecordBatch);

        // todo when PSM and PartitionState are refactored, these two calls in PS should be a single call
        state.pruneRemovedTrackedIncompleteOffsets(polledTestBatch.polledRecordBatch.records(tp));
        for (var wc : polledTestBatch.polledBatchWCs) {
            state.addNewIncompleteWorkContainer(wc);
        }

        //
        OffsetAndMetadata offsetAndMetadata = state.createOffsetAndMetadata();

        ManagedTruth.assertThat(offsetAndMetadata).getOffset().isEqualTo(0L);
        state.getAllIncompleteOffsets().containsAll(Range.range(highestSeenOffset).list());
    }

    /**
     * CG offset has been changed to something higher than expected (offset skip) (metdata lost?)
     */
    @Test
    void bootstrapPollOffsetHigher() {
        // committed state
        PartitionState<String, String> state = new PartitionState<>(tp, offsetData);

        // bootstrap poll
        PolledTestBatch polledTestBatch = new PolledTestBatch(mu, tp, highestSeenOffset);

        // todo when PSM and PartitionState are refactored, these two calls in PS should be a single call
        state.pruneRemovedTrackedIncompleteOffsets(polledTestBatch.polledRecordBatch.records(tp));
        for (var wc : polledTestBatch.polledBatchWCs) {
            state.addNewIncompleteWorkContainer(wc);
        }

        //
        OffsetAndMetadata offsetAndMetadata = state.createOffsetAndMetadata();

        ManagedTruth.assertThat(offsetAndMetadata).getOffset().isEqualTo(0L);
        state.getAllIncompleteOffsets().containsAll(Range.range(highestSeenOffset).list());
    }

    /**
     * CG offset has disappeared - committed offset hasn't been changed, but broker gives us a bootstrap poll result
     * with a higher offset than expected. Could be caused by retention period, or compaction.
     */
    @Test
    void committedOffsetRemoved() {
        // committed state
        PartitionState<String, String> state = new PartitionState<>(tp, offsetData);

        // bootstrap poll
        PolledTestBatch polledTestBatch = new PolledTestBatch(mu, tp, highestSeenOffset);

        // todo when PSM and PartitionState are refactored, these two calls in PS should be a single call
        state.pruneRemovedTrackedIncompleteOffsets(polledTestBatch.polledRecordBatch.records(tp));
        for (var wc : polledTestBatch.polledBatchWCs) {
            state.addNewIncompleteWorkContainer(wc);
        }

        //
        OffsetAndMetadata offsetAndMetadata = state.createOffsetAndMetadata();

        ManagedTruth.assertThat(offsetAndMetadata).getOffset().isEqualTo(0L);
        state.getAllIncompleteOffsets().containsAll(Range.range(highestSeenOffset).list());
    }


}