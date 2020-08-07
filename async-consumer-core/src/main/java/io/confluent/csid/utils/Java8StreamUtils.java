package io.confluent.csid.utils;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Java8StreamUtils {

    static public <T> Stream<T> setupStreamFromDeque(ConcurrentLinkedDeque<T> userProcessResultsStream) {
        Spliterator<T> spliterator = Spliterators.spliterator(new Iterator<>() {
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
        }, userProcessResultsStream.size(), Spliterator.NONNULL);

        return StreamSupport.stream(spliterator, false);
    }

}
