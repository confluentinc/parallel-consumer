package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * Class for simple ranges.
 * <p>
 * <a href="https://stackoverflow.com/a/16570509/105741">For loop - like Python range function</a>
 *
 * @see #range(long)
 */
public class Range implements Iterable<Long> {

    private final long limit;

    /**
     * Provides an {@link Iterable} for the range of numbers from 0 to the given limit.
     * <p>
     * Exclusive of max.
     * <p>
     * Consider using {@link IntStream#range(int, int)#forEachOrdered} instead:
     * <pre>
     * IntStream.range(0, originalBitsetSize).forEachOrdered(offset -> {
     * </pre>
     * However, if you don't want o use a closure, this is a good alternative.
     */
    public static Range range(long max) {
        return new Range(max);
    }

    public Range(long limit) {
        this.limit = limit;
    }

    /**
     * Potentially slow, but useful for tests
     */
    public static List<Integer> listOfIntegers(int max) {
        return Range.range(max).listAsIntegers();
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

    /**
     * Potentially slow, but useful for tests
     */
    public List<Integer> listAsIntegers() {
        ArrayList<Integer> integers = new ArrayList<>();
        forEach(e -> integers.add(Math.toIntExact(e)));
        return integers;
    }

    public LongStream toStream() {
        return LongStream.range(0, limit);
    }

}