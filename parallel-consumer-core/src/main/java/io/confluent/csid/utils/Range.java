package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * Range function for Java that provides an Iterable, not just an Iterator which {@link java.util.stream.LongStream#range#iterator()} can.
 * <p>
 * https://stackoverflow.com/a/16570509/105741
 *
 * @author Antony Stubbs
 * @deprecated use {@link java.util.stream.LongStream#range} instead
 */
@Deprecated
public class Range implements Iterable<Long> {

    private final long limit;

    public Range(long limit) {
        this.limit = limit;
    }

    @Override
    public Iterator<Long> iterator() {
        final long max = limit;
        return new Iterator<>() {

            private long current = 0;

            @Override
            public boolean hasNext() {
                return current < max;
            }

            @Override
            public Long next() {
                if (hasNext()) {
                    return current++;
                } else {
                    throw new NoSuchElementException("Range reached the end");
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Can't remove values from a Range");
            }
        };
    }

    public List<Integer> listAsIntegers() {
        ArrayList<Integer> integers = new ArrayList<>();
        forEach(e -> integers.add(Math.toIntExact(e)));
        return integers;
    }

    public LongStream toStream() {
        return LongStream.range(0, limit);
    }

    static void range(int max, IntConsumer consumer) {
        IntStream.range(0, max)
                .forEach(consumer);
    }

    /**
     * Exclusive of max
     */
    public static Range range(long max) {
        return new Range(max);
    }

}