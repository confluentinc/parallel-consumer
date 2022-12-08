package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.DrainingCloseable;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public interface JStreamParallelStreamProcessor<K, V> extends DrainingCloseable {

    static <K, V> JStreamParallelStreamProcessor<K, V> createJStreamEosStreamProcessor(ParallelConsumerOptions<K, V> options) {
        return new JStreamParallelEoSStreamProcessor<>(options);
    }

    /**
     * Like {@link AbstractParallelEoSStreamProcessor#pollAndProduceMany} but instead of callbacks, streams the results
     * instead, after the produce result is ack'd by Kafka.
     *
     * @return a stream of results of applying the function to the polled records
     */
    Stream<ParallelStreamProcessor.ConsumeProduceResult<K, V, K, V>> pollProduceAndStream(
            Function<PollContext<K, V>,
                    List<ProducerRecord<K, V>>> userFunction);
}
