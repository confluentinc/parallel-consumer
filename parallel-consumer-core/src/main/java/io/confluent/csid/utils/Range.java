package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toList;

/**
 * Class for simple ranges.
 * <p>
 * <a href="https://stackoverflow.com/a/16570509/105741">For loop - like Python range function</a>
 *
 * @see #range(long)
 */
public class Range implements Iterable<Long> {

    private final long start;

    private final long limit;

    /**
     * @see this#range(long)
     */
    public Range(int start, long max) {
        this.start = start;
        this.limit = max;
    }

    public Range(long limit) {
        this.start = 0L;
        this.limit = limit;
    }

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

    /**
     * @see #range(long)
     */
    public static Range range(int start, long max) {
        return new Range(start, max);
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

            private long current = start;

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
        return IntStream.range(Math.toIntExact(start), Math.toIntExact(limit))
                .boxed()
                .collect(toList());
    }

    public LongStream toStream() {
        return LongStream.range(start, limit);
    }

}