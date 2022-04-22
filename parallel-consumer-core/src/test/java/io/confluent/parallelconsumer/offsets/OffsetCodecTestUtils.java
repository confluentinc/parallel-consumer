package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.controller.PartitionState;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

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
    static Set<Long> bitmapStringToIncomplete(final long baseOffset, final String inputBitmapString) {
        final Set<Long> incompleteOffsets = new HashSet<>();

        final long longLength = inputBitmapString.length();
        range(longLength).forEach(i -> {
            var bit = inputBitmapString.charAt(i);
            if (bit == 'o') {
                incompleteOffsets.add(baseOffset + i);
            } else if (bit == 'x') {
                log.trace("Dropping completed offset");
            } else {
                throw new IllegalArgumentException("Invalid encoding - unexpected char: " + bit);
            }
        });

        return incompleteOffsets;
    }

}
