package io.confluent.parallelconsumer;

import io.confluent.parallelconsumer.RunLengthEncoder.RunLengthEntry;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.tlinkowski.unij.api.UniLists.*;

class RunLengthEncoderTest {

    /**
     * Starting with offsets and bit values:
     * <p>
     * 0 1   2 3 4 5 6  7 8 9  10  11 12 13  14 15 16 17
     * <p>
     * 0 0   1 1 0 1 1  0 0 0  1    0  0  0   1 1  1  1
     * <p>
     * The run lengths are: 2,2,1,2,3,1,3,4
     * <p>
     * If we were to need to truncate at offset 4 (the new base)
     * <p>
     * 4
     * <p>
     * The new offsets and bit values are:
     * <p>
     * 0  1 2  3 4 5  6
     * <p>
     * 0  1 1  0 0 0  1
     * <p>
     * Who's run lengths are:
     * <p>
     * 1, 2, 3, 1
     */
    @Disabled("V1 deprecated and doesn't currently work")
    @Test
    void truncateV1() {
        // v1
        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 10);

            assertOffsetsAndRuns(rl,
                    of(12, 13, 15, 16, 20, 24, 25, 26, 27), // offsets
                    of(2, 2, 1, 2, 3, 1, 3, 4)); // runs

            rl.truncateRunlengths(12);

            assertOffsetsAndRuns(rl,
                    of(2, 4), // offsets
                    of(14, 15, 16, 17)); // runs
        }

        //v1
        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 0);

            rl.truncateRunlengths(4);

            List<Integer> runLengthEncodingIntegers = rl.getRunLengthEncodingIntegers();
            assertThat(runLengthEncodingIntegers).containsExactly(1, 2, 3, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(5L, 6L, 10L, 14L, 15L, 16L, 17L);
        }

        // v1
        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 0);

            rl.truncateRunlengths(8);

            List<Integer> runLengthEncodingIntegers = rl.getRunLengthEncodingIntegers();
            assertThat(runLengthEncodingIntegers).containsExactly(2, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(10L, 14L, 15L, 16L, 17L);
        }


        // v1
        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 0);

            rl.truncateRunlengths(9);

            List<Integer> runLengthEncodingIntegers = rl.getRunLengthEncodingIntegers();
            assertThat(runLengthEncodingIntegers).containsExactly(1, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(10L, 14L, 15L, 16L, 17L);
        }
    }

    @Test
    void truncateV2() {
        {
            // test case where base != 0
            int base = 10;
            RunLengthEncoder rl = new RunLengthEncoder(base, new OffsetSimultaneousEncoder(base, (long) base), OffsetEncoding.Version.v2);

            encodePattern(rl, base);

            assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getStartOffset).extracting(Long::intValue).containsExactly(10, 12, 14, 15, 17, 20, 21, 24);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(12L, 13L, 15L, 16L, 20L, 24L, 25L, 26L, 27L);

            rl.truncateRunlengthsV2(22);

            assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getRunLength).containsExactly(2, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(24L, 25L, 26L, 27L);
        }

        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 0);

            rl.truncateRunlengthsV2(4);

            assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getRunLength).containsExactly(1, 2, 3, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(5L, 6L, 10L, 14L, 15L, 16L, 17L);
        }

        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 0);

            rl.truncateRunlengthsV2(8);

            assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getRunLength).containsExactly(2, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(10L, 14L, 15L, 16L, 17L);
        }


        {
            RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 0L), OffsetEncoding.Version.v2);

            encodePattern(rl, 0);

            rl.truncateRunlengthsV2(9);

            assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getRunLength).containsExactly(1, 1, 3, 4);
            assertThat(rl.calculateSucceededActualOffsets()).containsExactly(10L, 14L, 15L, 16L, 17L);
        }
    }

    private void encodePattern(final RunLengthEncoder rl, long base) {
        int highest = 17 + (int) base;
        int relative = 0;
        {
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;

            rl.encodeCompleteOffset(base, relative, highest);
            relative++;
            rl.encodeCompleteOffset(base, relative, highest);
            relative++;

            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;

            rl.encodeCompleteOffset(base, relative, highest);
            relative++;
            rl.encodeCompleteOffset(base, relative, highest);
            relative++;

            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;

            rl.encodeCompleteOffset(base, relative, highest);
            relative++;

            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;
            rl.encodeIncompleteOffset(base, relative, highest);
            relative++;

            rl.encodeCompleteOffset(base, relative, highest);
            relative++;
            rl.encodeCompleteOffset(base, relative, highest);
            relative++;
            rl.encodeCompleteOffset(base, relative, highest);
            relative++;
            rl.encodeCompleteOffset(base, relative, highest);
        }
    }

    /**
     * We receive a truncation request where the truncation point is beyond anything our runlengths cover
     */
    @Test
    void v2TruncateOverMax() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeIncompleteOffset(0, 0, 0);
        rl.encodeCompleteOffset(0, 1, 1);

//        rl.addTail();
        rl.truncateRunlengthsV2(2);

        assertThat(rl.runLengthOffsetPairs).isEmpty();
        assertThat(rl.calculateSucceededActualOffsets()).isEmpty();
    }

    /**
     * Encoding requests can be out of order
     */
    @Test
    void outOfOrderEncoding() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 10, 10);
        rl.encodeCompleteOffset(0, 11, 11);
        rl.encodeCompleteOffset(0, 12, 12);
        assertOffsetsAndRuns(rl,
                of(10, 11, 12), // offsets
                of(10, 3)); // runs

        // middle offset out of order
        rl.encodeCompleteOffset(0, 6, 12);
        assertOffsetsAndRuns(rl,
                of(6, 10, 11, 12), // offsets
                of(6, 1, 3, 3)); // runs

        // 0 case offset out of order
        rl.encodeCompleteOffset(0, 1, 12);
        assertOffsetsAndRuns(rl,
                of(1, 6, 10, 11, 12), // offsets
                of(1, 1, 4, 1, 3, 3)); // runs

        rl.truncateRunlengthsV2(2);
        assertOffsetsAndRuns(rl,
                of(6, 10, 11, 12), // offsets
                of(4, 1, 3, 3)); // runs

        rl.truncateRunlengthsV2(8);
        assertOffsetsAndRuns(rl,
                of(10, 11, 12), // offsets
                of(2, 3)); // runs
    }

    /**
     * Segmentation of existing entry on out of order arrival
     * <p>
     * Scenarios:
     * <p>
     * offset range - run length - bit O or X (x=success)
     * <p>
     * one:
     * <p>
     * 10-20 - 3 O
     * <p>
     * 16 -1 O // ignore - impossible?
     * <p>
     * two:
     * <p>
     * 10-20 - 3 X
     * <p>
     * 16 -1 X // ignore?
     * <p>
     * three:
     * <p>
     * 10-20 - 3 O
     * <p>
     * 16 -1 X results in:
     * <p>
     * 10-15 - 5 O 16-16 - 1 X 17-20 - 4 O
     * <p>
     * Four:
     * <p>
     * 10-20 - 3 X
     * <p>
     * 16 -1 O // impossible - ignore*
     */
    @Test
    void segmentTestOne() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 10, 10);
        assertOffsetsAndRuns(rl,
                of(10), // offsets
                of(10, 1)); // runs

        // middle offset out of order
        rl.encodeCompleteOffset(0, 6, 10);
        assertOffsetsAndRuns(rl,
                of(6, 10),
                of(6, 1, 3, 1));

        assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getStartOffset).extracting(Long::intValue).containsExactly(0, 6, 7, 10);
        assertThat(rl.runLengthOffsetPairs).extracting(RunLengthEntry::getRunLength).containsExactly(6, 1, 3, 1);
        assertThat(rl.calculateSucceededActualOffsets()).extracting(Long::intValue).containsExactly(6, 10);
    }

    @Test
    void segmentTestTwo() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(10, 20, 20);
        assertOffsetsAndRuns(rl,
                of(30), // successful offset
                of(20, 1) // run lengths
        );

        // middle
        rl.encodeCompleteOffset(10, 10, 20);
        assertOffsetsAndRuns(rl,
                of(20, 30),
                of(10, 1, 9, 1));

        // middle offset out of order
        rl.encodeCompleteOffset(10, 6, 20);
        assertOffsetsAndRuns(rl,
                of(16, 20, 30),
                of(6, 1, 3, 1, 9, 1));

        // up by one, continuous 6 and 7 combine to one run
        rl.encodeCompleteOffset(10, 7, 20);
        assertOffsetsAndRuns(rl,
                of(16, 17, 20, 30),
                of(6, 2, 2, 1, 9, 1));

        // down by one, continuous so combine
        rl.encodeCompleteOffset(10, 5, 20);
        assertOffsetsAndRuns(rl,
                of(15, 16, 17, 20, 30),
                of(5, 3, 2, 1, 9, 1));

        // add a big gap maker
        rl.encodeCompleteOffset(10, 35, 45);
        assertOffsetsAndRuns(rl,
                of(15, 16, 17, 20, 30, 45),
                of(5, 3, 2, 1, 9, 1, 14, 1));

        // off by one higher, continuous so combine
        rl.encodeCompleteOffset(10, 36, 46);
        assertOffsetsAndRuns(rl,
                of(15, 16, 17, 20, 30, 45, 46),
                of(5, 3, 2, 1, 9, 1, 14, 2));

        // off by one gap
        rl.encodeCompleteOffset(10, 38, 48);
        assertOffsetsAndRuns(rl,
                of(15, 16, 17, 20, 30, 45, 46, 48),
                of(5, 3, 2, 1, 9, 1, 14, 2, 1, 1));
    }

    private void assertOffsetsAndRuns(final RunLengthEncoder rl, List<Integer> goodOffsets, List<Integer> runs) {
        assertThat(rl.runLengthOffsetPairs)
                .as("run lengths")
                .extracting(RunLengthEntry::getRunLength)
                .containsExactlyElementsOf(runs);

        assertThat(rl.calculateSucceededActualOffsets()).as("succeeded Offsets")
                .extracting(Long::intValue)
                .containsExactlyElementsOf(goodOffsets);

    }

    /**
     * Has to combine 3 run lengths into 1, both from above and below
     */
    @Test
    void segmentTestThree() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(10, 20, 20);
        assertOffsetsAndRuns(rl,
                of(30),
                of(20, 1));

        rl.encodeCompleteOffset(10, 14, 20);
        assertOffsetsAndRuns(rl,
                of(24, 30),
                of(14, 1, 5, 1));

        rl.encodeCompleteOffset(10, 16, 20);
        assertOffsetsAndRuns(rl,
                of(24, 26, 30),
                of(14, 1, 1, 1, 3, 1));

        //
        rl.encodeCompleteOffset(10, 15, 20);
        assertOffsetsAndRuns(rl,
                of(24, 25, 26, 30),
                of(14, 3, 3, 1));
    }

    /**
     * Has to combine 2 run lengths into 1
     */
    @Test
    void segmentTestFour() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 5, 5);
        assertOffsetsAndRuns(rl,
                of(5),
                of(5, 1));

        rl.encodeCompleteOffset(0, 6, 6);
        assertOffsetsAndRuns(rl,
                of(5, 6),
                of(5, 2));
    }

    /**
     * Has to combine 2 run lengths into 1. Over
     */
    @Test
    void segmentTestFiveOver() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 4, 4);
        assertOffsetsAndRuns(rl,
                of(4),
                of(4, 1));

        //
        rl.encodeCompleteOffset(0, 5, 5);
        assertOffsetsAndRuns(rl,
                of(4, 5),
                of(4, 2));
    }

    /**
     * Has to combine 2 run lengths into 1. Under
     */
    @Test
    void segmentTestFiveUnder() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 4, 4);
        assertOffsetsAndRuns(rl,
                of(4),
                of(4, 1));

        //
        rl.encodeCompleteOffset(0, 3, 4);
        assertOffsetsAndRuns(rl,
                of(3, 4),
                of(3, 2));
    }

    /**
     * Has to combine 2 run lengths into 1. Over
     */
    @Test
    void segmentTestFiveOverMultiple() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 4, 5);
        assertOffsetsAndRuns(rl,
                of(4),
                of(4, 1));

        //
        rl.encodeCompleteOffset(0, 5, 5);
        assertOffsetsAndRuns(rl,
                of(4, 5),
                of(4, 2));

        rl.encodeCompleteOffset(0, 6, 6);
        assertOffsetsAndRuns(rl,
                of(4, 5, 6),
                of(4, 3));

        rl.encodeCompleteOffset(0, 7, 7);
        assertOffsetsAndRuns(rl,
                of(4, 5, 6, 7),
                of(4, 4));
    }

    /**
     * Has to combine both up and down
     */
    @Test
    void segmentTestFixUpAndDownSimpleUpwards() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 4, 4);
        assertOffsetsAndRuns(rl,
                of(4),
                of(4, 1));

        //
        rl.encodeCompleteOffset(0, 6, 6);
        assertOffsetsAndRuns(rl,
                of(4, 6),
                of(4, 1, 1, 1));

        rl.encodeCompleteOffset(0, 5, 6);
        assertOffsetsAndRuns(rl,
                of(4, 5, 6),
                of(4, 3));
    }

    /**
     * Has to combine both up and down
     */
    @Test
    void segmentTestFixUpAndDownSimpleDownwards() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 6, 6);
        assertOffsetsAndRuns(rl,
                of(6),
                of(6, 1));

        //
        rl.encodeCompleteOffset(0, 4, 6);
        assertOffsetsAndRuns(rl,
                of(4, 6),
                of(4, 1, 1, 1));

        rl.encodeCompleteOffset(0, 5, 6);
        assertOffsetsAndRuns(rl,
                of(4, 5, 6),
                of(4, 3));
    }

    @Test
    void segmentTestThick() {
        RunLengthEncoder rl = new RunLengthEncoder(0, new OffsetSimultaneousEncoder(0, 1L), OffsetEncoding.Version.v2);

        rl.encodeCompleteOffset(0, 6, 6);
        rl.encodeCompleteOffset(0, 7, 7);
        rl.encodeCompleteOffset(0, 9, 9);
        assertOffsetsAndRuns(rl,
                of(6, 7, 9),
                of(6, 2, 1, 1));

        //
        rl.encodeCompleteOffset(0, 4, 9);
        assertOffsetsAndRuns(rl,
                of(4, 6, 7, 9),
                of(4, 1, 1, 2, 1, 1));

        rl.encodeCompleteOffset(0, 3, 9);
        assertOffsetsAndRuns(rl,
                of(3, 4, 6, 7, 9),
                of(3, 2, 1, 2, 1, 1));

        rl.encodeCompleteOffset(0, 5, 9);
        assertOffsetsAndRuns(rl,
                of(3, 4, 5, 6, 7, 9),
                of(3, 5, 1, 1));
    }
}
