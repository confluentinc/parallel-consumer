package io.confluent.csid.asyncconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.asyncconsumer.AsyncConsumer.Tuple;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@UtilityClass
public class OffsetRunLength {

    /**
     * @return run length encoding, always starting with an 'o' count
     */
    static List<Integer> runLengthEncode(final String in) {
        final AtomicInteger length = new AtomicInteger();
        final AtomicBoolean previous = new AtomicBoolean(false);
        final List<Integer> encoding = new ArrayList<>();
        in.chars().forEachOrdered(bit -> {
            final boolean current = switch (bit) {
                case 'o' -> false;
                case 'x' -> true;
                default -> throw new IllegalArgumentException(bit + " in " + in);
            };
            if (previous.get() == current) {
                length.getAndIncrement();
            } else {
                previous.set(current);
                encoding.add(length.get());
                length.set(1);
            }
        });
        encoding.add(length.get()); // add tail
        return encoding;
    }

    /**
     * @see #runLengthEncode
     */
    static String runLengthDecodeToString(final List<Integer> in) {
        final StringBuilder sb = new StringBuilder(in.size());
        boolean current = false;
        for (final Integer i : in) {
            for (int x = 0; x < i; x++) {
                if (current) {
                    sb.append('x');
                } else {
                    sb.append('o');
                }
            }
            current = !current; // toggle
        }
        return sb.toString();
    }


    /**
     * @see #runLengthEncode
     */
    static Tuple<Long, Set<Long>> runLengthDecodeToIncompletes(final long baseOffset, final ByteBuffer in) {
        in.rewind();
        final ShortBuffer shortBuffer = in.asShortBuffer();

        final var incompletes = new HashSet<Long>(1); // we don't know the capacity yet

        long highestWatermarkSeen = 0L;

        if (log.isTraceEnabled()) {
            var shorts = new ArrayList<Short>();
            while (shortBuffer.hasRemaining()) {
                shorts.add(shortBuffer.get());
            }
            log.debug("Unrolled shorts: {}", shorts);
            shortBuffer.rewind();
        }

        //
        boolean currentRunlengthIsComplete = false;
        long currentOffset = baseOffset;
        while (shortBuffer.hasRemaining()) {
            short runLength = shortBuffer.get();
            highestWatermarkSeen = currentOffset + runLength;
            if (currentRunlengthIsComplete) {
                log.trace("Ignoring {} completed offset", runLength);
                currentOffset += runLength;
            } else {
                log.trace("Adding {} incomplete offset", runLength);
                for (int relativeOffset = 0; relativeOffset < runLength; relativeOffset++) {
                    incompletes.add(currentOffset);
                    currentOffset++;
                }
            }
            currentRunlengthIsComplete = !currentRunlengthIsComplete; // toggle
        }
        return Tuple.pairOf(highestWatermarkSeen, incompletes);
    }

    /**
     * TODO: Is there an advantage to keeping it as an list of Shorts instead of Integer?
     */
    static List<Integer> runLengthDeserialise(final ByteBuffer in) {
        // view as short buffer
        in.rewind();
        final ShortBuffer shortBuffer = in.asShortBuffer();

        //
        final List<Integer> results = new ArrayList<>(shortBuffer.capacity());
        while (shortBuffer.hasRemaining()) {
            results.add((int) shortBuffer.get());
        }
        return results;
    }

}
