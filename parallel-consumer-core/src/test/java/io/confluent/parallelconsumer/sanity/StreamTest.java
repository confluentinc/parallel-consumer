package io.confluent.parallelconsumer.sanity;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Sanity test usage of Java {@link Stream}
 */
@Slf4j
public class StreamTest {

    //    @Test
    public void test() {
        Stream<Double> s = Stream.generate(() -> Math.random());
        s.forEach(x -> {
            log.info(x.toString());
        });
    }

    @Test
    public void testStreamSpliterators() {
        int max = 10;

        Iterator<String> i = new Iterator<>() {

            int count = 0;

            @Override
            public boolean hasNext() {
                return count < max;
            }

            @Override
            public String next() {
                count++;
                return count + " " + Math.random();

            }
        };

        Spliterator<String> spliterator = Spliterators.spliterator(i, 0, Spliterator.NONNULL);

        Stream<String> stream = StreamSupport.stream(spliterator, false);

        List<String> collect = stream
                .map(x -> {
                    log.info(x);
                            return x.toUpperCase();
                        }
                )
                .collect(Collectors.toList());

        Assertions.assertThat(collect).hasSize(max);
    }

}
