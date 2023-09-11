package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.experimental.UtilityClass;

import java.util.Deque;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@UtilityClass
public class Java8StreamUtils {

    public static <T> Stream<T> setupStreamFromDeque(Deque<? extends T> userProcessResultsStream) {
        Spliterator<T> spliterator = Spliterators.spliterator(new DequeIterator<T>(userProcessResultsStream), userProcessResultsStream.size(), Spliterator.NONNULL);

        return StreamSupport.stream(spliterator, false);
    }

    private static class DequeIterator<T> implements Iterator<T> {

        private final Deque<? extends T> userProcessResultsStream;

        public DequeIterator(Deque<? extends T> userProcessResultsStream) {
            this.userProcessResultsStream = userProcessResultsStream;
        }

        @Override
        public boolean hasNext() {
            boolean notEmpty = !userProcessResultsStream.isEmpty();
            return notEmpty;
        }

        @Override
        public T next() {
            T poll = userProcessResultsStream.poll();
            return poll;
        }
    }
}
