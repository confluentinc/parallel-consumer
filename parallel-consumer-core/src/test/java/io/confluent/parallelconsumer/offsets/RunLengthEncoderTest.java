package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.Range;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.SneakyThrows;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.JavaUtils.toTreeSet;
import static io.confluent.parallelconsumer.offsets.OffsetEncoding.Version.v2;
import static org.assertj.core.api.Assertions.assertThat;

class RunLengthEncoderTest {

    /**
     * Check that run length supports gaps in the source partition - i.e. compacted topics where offsets aren't strictly
     * sequential
     */
    @SneakyThrows
    @Test
    void noGaps() {
        var incompletes = UniSets.of(0, 4, 6, 7, 8, 10).stream().map(x -> (long) x).collect(toTreeSet());
        var completes = UniSets.of(1, 2, 3, 5, 9).stream().map(x -> (long) x).collect(toTreeSet());
        List<Integer> runs = UniLists.of(1, 3, 1, 1, 3, 1, 1);
        OffsetSimultaneousEncoder offsetSimultaneousEncoder = new OffsetSimultaneousEncoder(-1, 0L, incompletes);

        {
            RunLengthEncoder rl = new RunLengthEncoder(offsetSimultaneousEncoder, v2);

            rl.encodeIncompleteOffset(0); // 1
            rl.encodeCompletedOffset(1); // 3
            rl.encodeCompletedOffset(2);
            rl.encodeCompletedOffset(3);
            rl.encodeIncompleteOffset(4); // 1
            rl.encodeCompletedOffset(5); // 1
            rl.encodeIncompleteOffset(6); // 3
            rl.encodeIncompleteOffset(7);
            rl.encodeIncompleteOffset(8);
            rl.encodeCompletedOffset(9); // 1
            rl.encodeIncompleteOffset(10); // 1

            rl.addTail();

            // before serialisation
            {
                assertThat(rl.getRunLengthEncodingIntegers()).containsExactlyElementsOf(runs);

                List<Long> calculatedCompletedOffsets = rl.calculateSucceededActualOffsets(0);

                assertThat(calculatedCompletedOffsets).containsExactlyElementsOf(completes);
            }
        }
    }


    /**
     * Check that run length supports gaps in the source partition - i.e. compacted topics where offsets aren't strictly
     * sequential
     */
    @SneakyThrows
    @Test
    void noGapsSerialisation() {
        var incompletes = UniSets.of(0, 4, 6, 7, 8, 10).stream().map(x -> (long) x).collect(toTreeSet()); // lol - DRY!
        var completes = UniSets.of(1, 2, 3, 5, 9).stream().map(x -> (long) x).collect(toTreeSet()); // lol - DRY!
        List<Integer> runs = UniLists.of(1, 3, 1, 1, 3, 1, 1);
        OffsetSimultaneousEncoder offsetSimultaneousEncoder = new OffsetSimultaneousEncoder(-1, 0L, incompletes);

        {
            RunLengthEncoder rl = new RunLengthEncoder(offsetSimultaneousEncoder, v2);

            rl.encodeIncompleteOffset(0); // 1
            rl.encodeCompletedOffset(1); // 3
            rl.encodeCompletedOffset(2);
            rl.encodeCompletedOffset(3);
            rl.encodeIncompleteOffset(4); // 1
            rl.encodeCompletedOffset(5); // 1
            rl.encodeIncompleteOffset(6); // 3
            rl.encodeIncompleteOffset(7);
            rl.encodeIncompleteOffset(8);
            rl.encodeCompletedOffset(9); // 1
            rl.encodeIncompleteOffset(10); // 1

            // after serialisation
            {
                byte[] raw = rl.serialise();

                byte[] wrapped = offsetSimultaneousEncoder.packEncoding(new EncodedOffsetPair(OffsetEncoding.RunLengthV2, ByteBuffer.wrap(raw)));

                HighestOffsetAndIncompletes result = OffsetMapCodecManager.decodeCompressedOffsets(0, wrapped);

                assertThat(result.getHighestSeenOffset()).contains(10L);

                assertThat(result.getIncompleteOffsets()).containsExactlyElementsOf(incompletes);
            }
        }
    }

    /**
     * Check that run length supports gaps in the source partition - i.e. compacted topics where offsets aren't strictly
     * sequential.
     */
    @SneakyThrows
    @Test
    void gapsInOffsetsWork() {
        var incompletes = UniSets.of(0, 6, 10).stream().map(x -> (long) x).collect(toTreeSet());

        // NB: gaps between completed offsets get encoded as succeeded offsets. This doesn't matter because they don't exist and we'll neve see them.
        Set<Long> completes = UniSets.of(1, 2, 3, 4, 5, 9).stream().map(x -> (long) x).collect(Collectors.toSet());
        List<Integer> runs = UniLists.of(1, 5, 3, 1, 1);
        OffsetSimultaneousEncoder offsetSimultaneousEncoder = new OffsetSimultaneousEncoder(-1, 0L, incompletes);

        {
            RunLengthEncoder rl = new RunLengthEncoder(offsetSimultaneousEncoder, v2);

            rl.encodeIncompleteOffset(0);
            rl.encodeCompletedOffset(1);
            // gap completes at 2
            rl.encodeCompletedOffset(3);
            rl.encodeCompletedOffset(4);
            rl.encodeCompletedOffset(5);
            rl.encodeIncompleteOffset(6);
            // gap incompletes at 7
            rl.encodeIncompleteOffset(8);
            rl.encodeCompletedOffset(9);
            rl.encodeIncompleteOffset(10);

            rl.addTail();

            assertThat(rl.getRunLengthEncodingIntegers()).containsExactlyElementsOf(runs);

            List<Long> calculatedCompletedOffsets = rl.calculateSucceededActualOffsets(0);

            assertThat(calculatedCompletedOffsets).containsExactlyElementsOf(completes);
        }
    }


    /**
     * Check RLv2 errors on integer overflow
     */
    @SneakyThrows
    @ParameterizedTest()
    @EnumSource(OffsetEncoding.Version.class)
    void vTwoIntegerOverflow(OffsetEncoding.Version versionToTest) {
        final long integerMaxOverflowOffset = 100;
        final long overflowedValue = Integer.MAX_VALUE + integerMaxOverflowOffset;

        var incompletes = UniSets.of(0L, 4L, 6L, 7L, 8L, 10L, overflowedValue).stream().collect(toTreeSet());
        var completes = UniSets.of(1, 2, 3, 5, 9).stream().map(x -> (long) x).collect(toTreeSet());
        OffsetSimultaneousEncoder offsetSimultaneousEncoder
                = new OffsetSimultaneousEncoder(-1, overflowedValue - 1, incompletes);

        {
            final OffsetEncoding.Version versionsToTest = v2;
            testRunLength(overflowedValue, offsetSimultaneousEncoder, versionToTest);
        }
    }

    private static void testRunLength(long overflowedValue, OffsetSimultaneousEncoder offsetSimultaneousEncoder, OffsetEncoding.Version versionsToTest) throws EncodingNotSupportedException {
        RunLengthEncoder rl = new RunLengthEncoder(offsetSimultaneousEncoder, versionsToTest);

        rl.encodeIncompleteOffset(0); // 1
        rl.encodeCompletedOffset(1); // 3
        rl.encodeCompletedOffset(2);
        rl.encodeCompletedOffset(3);
        rl.encodeIncompleteOffset(4); // 1
        rl.encodeCompletedOffset(5); // 1
        rl.encodeIncompleteOffset(6); // 3
        rl.encodeIncompleteOffset(7);
        rl.encodeIncompleteOffset(8);
        rl.encodeCompletedOffset(9); // 1
        rl.encodeIncompleteOffset(10); // 1

        // inject overflow offset
        var errorAssertion = Assertions.assertThatThrownBy(() -> {
            for (var relativeOffset : Range.range(11, overflowedValue)) {
                rl.encodeCompletedOffset(relativeOffset);
            }
        });

        switch (versionsToTest) {
            case v1 -> {
                errorAssertion.isInstanceOf(RunLengthV1EncodingNotSupported.class);
                errorAssertion.hasMessageContainingAll("too big", "Short");
            }
            case v2 -> {
                errorAssertion.isInstanceOf(RunLengthV2EncodingNotSupported.class);
                errorAssertion.hasMessageContainingAll("too big", "Integer");
            }
        }

    }
}
