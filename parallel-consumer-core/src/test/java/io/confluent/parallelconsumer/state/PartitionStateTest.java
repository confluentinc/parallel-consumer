package io.confluent.parallelconsumer.state;

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

class PartitionStateTest {

    ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

    /**
     * @see PartitionState#maybeTruncateBelow
     */
    @Test
    void bootstrapTruncation() {
        TopicPartition tp = new TopicPartition("topic", 0);
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
        state.addWorkContainer(w20);


        Truth.assertThat(state.getNextExpectedInitialPolledOffset()).isEqualTo(unexpectedlyHighOffset);

        Truth.assertThat(state.getIncompleteOffsetsBelowHighestSucceeded()).containsExactlyElementsIn(expectedTruncatedIncompletes);

    }

}