package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class PartitionStateCommittedOffsetTest {

    ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

    TopicPartition tp = new TopicPartition("topic", 0);

    /**
     * @see PartitionState#maybeTruncateBelow
     */
    // todo parameter test with offsets closer together to check off by one
    @Test
    void bootstrapTruncation() {
        long unexpectedlyHighOffset = 20L;
        final long previouslyCommittedOffset = 11L;
        List<Long> incompletes = UniLists.of(previouslyCommittedOffset, 15L, unexpectedlyHighOffset, 60L, 80L);
        List<Long> expectedTruncatedIncompletes = incompletes.stream()
                .filter(offset -> offset >= unexpectedlyHighOffset)
                .collect(Collectors.toList());

        HighestOffsetAndIncompletes offsetData = new HighestOffsetAndIncompletes(Optional.of(100L), new HashSet<>(incompletes)); // todo fix set/list

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
    void compactedTopic() {
        Truth.assertThat(true).isFalse();
    }

    /**
     * CG offset has been changed to a lower offset (partition rewind / replay) (metdata lost?)
     */
    void committedOffsetLower() {
    }

    /**
     * CG offset has been changed to something higher than expected (offset skip) (metdata lost?)
     */
    void committedOffsetHigher() {
    }

    /**
     * CG offset has disappeared - committed offset hasn't been changed, but broker gives us a bootstrap poll result
     * with a higher offset than expected. Could be caused by retention period, or compaction.
     */
    void committedOffsetRemoved() {
    }


}