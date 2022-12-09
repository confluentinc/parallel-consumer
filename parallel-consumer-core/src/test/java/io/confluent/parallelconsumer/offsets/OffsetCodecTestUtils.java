package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.PartitionState;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.TreeSet;

import static io.confluent.csid.utils.Range.range;

@Slf4j
@UtilityClass
public class OffsetCodecTestUtils {

    /**
     * x is complete
     * <p>
     * o is incomplete
     */
    static String incompletesToBitmapString(long finalOffsetForPartition, long highestSeen, Set<Long> incompletes) {
        var runLengthString = new StringBuilder();
        Long lowWaterMark = finalOffsetForPartition;
        long end = highestSeen - lowWaterMark;
        for (final var relativeOffset : range(end)) {
            long offset = lowWaterMark + relativeOffset;
            if (incompletes.contains(offset)) {
                runLengthString.append("o");
            } else {
                runLengthString.append("x");
            }
        }
        return runLengthString.toString();
    }

    static String incompletesToBitmapString(long finalOffsetForPartition, PartitionState<?, ?> state) {
        return incompletesToBitmapString(finalOffsetForPartition,
                state.getOffsetHighestSeen(), state.getIncompleteOffsetsBelowHighestSucceeded());
    }

    /**
     * x is complete
     * <p>
     * o is incomplete
     */
    static TreeSet<Long> bitmapStringToIncomplete(final long baseOffset, final String inputBitmapString) {
        var incompleteOffsets = new TreeSet<Long>();

        final long longLength = inputBitmapString.length();
        range(longLength).forEach(index -> {
            var bit = inputBitmapString.charAt(Math.toIntExact(index));
            if (bit == 'o') {
                incompleteOffsets.add(baseOffset + index);
            } else if (bit == 'x') {
                log.trace("Dropping completed offset");
            } else {
                throw new IllegalArgumentException("Invalid encoding - unexpected char: " + bit);
            }
        });

        return incompleteOffsets;
    }

}
