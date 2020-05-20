package io.confluent.csid.asyncconsumer;

import io.confluent.csid.asyncconsumer.AsyncConsumerTest.MyInput;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Sanity test usage
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

        Iterator<MyInput> i = new Iterator<>() {

            int count = 0;

            @Override
            public boolean hasNext() {
                return count < max;
            }

            @Override
            public MyInput next() {
                count++;
                return new MyInput(count + " " + Math.random());

            }
        };

        Spliterator<MyInput> spliterator = Spliterators.spliterator(i, 0, Spliterator.NONNULL);

        Stream<MyInput> stream = StreamSupport.stream(spliterator, false);

//        Stream<MyInput> streamParallel = StreamSupport.stream(spliterator, true);

        List<String> collect = stream
                .map(x -> {
                            log.info(x.toString());
                            return x.getData().toUpperCase();
                        }
                )
                .collect(Collectors.toList());

        Assertions.assertThat(collect).hasSize(max);
    }
}
