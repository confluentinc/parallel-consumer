package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * https://stackoverflow.com/a/16570509/105741
 */
public class Range implements Iterable<Integer> {

    private final long limit;

    public Range(long limit) {
        this.limit = limit;
    }

    /**
     * Exclusive of max
     */
    public static Range range(long max) {
        return new Range(max);
    }

    @Override
    public Iterator<Integer> iterator() {
        final long max = limit;
        return new Iterator<Integer>() {

            private int current = 0;

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