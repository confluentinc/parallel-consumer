package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.InternalRuntimeError;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * for (var i : range) {}
 * <p>
 * https://stackoverflow.com/a/16570509/105741
 */
public class Range implements Iterable<Integer> {

    private int base = 0;
    private int limit;

    /**
     * @param limit exclusive
     */
    public Range(long limit) {
        this.limit = (int) limit;
        checkIntegerOverflow(this.limit, limit);
    }

    /**
     * @param base inclusive
     * @param limit exclusive
     */
    public Range(long base, long limit) {
        this.base = (int) base;
        checkIntegerOverflow(this.base, base);
        this.limit = (int) limit;
        checkIntegerOverflow(this.limit, limit);
    }

    private void checkIntegerOverflow(final int actual, final long expected) {
        if (actual != expected)
            throw new InternalRuntimeError(StringUtils.msg("Overflow {} from {}", actual, expected));
    }

    public static Range range(long max) {
        return new Range(max);
    }

    @Override
    public Iterator<Integer> iterator() {
        final long max = limit;
        return new Iterator<Integer>() {

            private int current = base;

            @Override
            public boolean hasNext() {
                return current < max;
            }

            @Override
            public Integer next() {
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

    public List<Integer> list() {
        ArrayList<Integer> integers = new ArrayList<Integer>();
        forEach(integers::add);
        return integers;
    }

    public IntStream toStream() {
        return IntStream.range(0, (int) limit);
    }

    static IntStream rangeStream(int i) {
        return IntStream.range(0, i);
    }

    static void range(int max, Consumer<Integer> consumer) {
        IntStream.range(0, max)
                .forEach(consumer::accept);
    }

}