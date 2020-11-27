package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

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
    static HighestOffsetAndIncompletes runLengthDecodeToIncompletes(OffsetEncoding encoding, final long baseOffset, final ByteBuffer in) {
        in.rewind();
        final ShortBuffer v1ShortBuffer = in.asShortBuffer();
        final IntBuffer v2IntegerBuffer = in.asIntBuffer();

        final var incompletes = new HashSet<Long>(1); // we don't know the capacity yet

        long highestSeenOffset = 0L;

        Supplier<Boolean> hasRemainingTest = () -> {
            return switch (encoding.version) {
                case v1 -> v1ShortBuffer.hasRemaining();
                case v2 -> v2IntegerBuffer.hasRemaining();
            };
        };
        if (log.isTraceEnabled()) {
            // print out all run lengths
            var runlengths = new ArrayList<Number>();
            try {
                while (hasRemainingTest.get()) {
                    Number runLength = switch (encoding.version) {
                        case v1 -> v1ShortBuffer.get();
                        case v2 -> v2IntegerBuffer.get();
                    };
                    runlengths.add(runLength);
                }
            } catch (BufferUnderflowException u) {
                log.error("Error decoding offsets", u);
            }
            log.debug("Unrolled runlengths: {}", runlengths);
            v1ShortBuffer.rewind();
            v2IntegerBuffer.rewind();
        }

        // decodes incompletes
        boolean currentRunlengthIsComplete = false;
        long currentOffset = baseOffset;
        while (hasRemainingTest.get()) {
            try {
                Number runLength = switch (encoding.version) {
                    case v1 -> v1ShortBuffer.get();
                    case v2 -> v2IntegerBuffer.get();
                };
                highestSeenOffset = currentOffset + runLength.longValue();

                if (currentRunlengthIsComplete) {
                    log.trace("Ignoring {} completed offset", runLength);
                    currentOffset += runLength.longValue();
                } else {
                    log.trace("Adding {} incomplete offset", runLength);
                    for (int relativeOffset = 0; relativeOffset < runLength.longValue(); relativeOffset++) {
                        incompletes.add(currentOffset);
                        currentOffset++;
                    }
                }
            } catch (BufferUnderflowException u) {
                log.error("Error decoding offsets", u);
                throw u;
            }
            currentRunlengthIsComplete = !currentRunlengthIsComplete; // toggle
        }
        return HighestOffsetAndIncompletes.of(highestSeenOffset, incompletes);
    }

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
