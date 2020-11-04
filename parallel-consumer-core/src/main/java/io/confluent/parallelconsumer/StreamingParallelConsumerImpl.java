package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.Java8StreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Function;
import java.util.stream.Stream;

@Slf4j
public class StreamingParallelConsumerImpl<K, V> extends ParallelConsumerImpl<K, V> {

    final private Stream<ConsumeProduceResult<K, V, K, V>> stream;

    final private ConcurrentLinkedDeque<ConsumeProduceResult<K, V, K, V>> userProcessResultsStream;

    public StreamingParallelConsumerImpl(org.apache.kafka.clients.consumer.Consumer<K, V> consumer,
                                         Producer<K, V> producer,
                                         ParallelConsumerOptions parallelConsumerOptions) {
        super(consumer, producer, parallelConsumerOptions);

        userProcessResultsStream = new ConcurrentLinkedDeque<>();

        stream = Java8StreamUtils.setupStreamFromDeque(userProcessResultsStream);
    }

    /**
     * Like {@link ParallelConsumerImpl#pollAndProduce} but instead of callbacks, streams the results instead, after the
     * produce result is ack'd by Kafka.
     *
     * @return a stream of results of applying the function to the polled records
     */
    public Stream<ConsumeProduceResult<K, V, K, V>> pollProduceAndStream(Function<ConsumerRecord<K, V>, List<ProducerRecord<K, V>>> userFunction) {
        super.pollAndProduce(userFunction, (result) -> {
            log.trace("Wrapper callback applied, sending result to stream. Input: {}", result);
            userProcessResultsStream.add(result);
        });

        return stream;
    }

}
