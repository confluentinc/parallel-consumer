package io.confluent.csid.asyncconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.Java8StreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
public class StreamingAsyncConsumer<K, V> extends AsyncConsumer<K, V> {

    final private Stream<ConsumeProduceResult<K, V, K, V>> stream;

    final private ConcurrentLinkedDeque<ConsumeProduceResult<K, V, K, V>> userProcessResultsStream;

    public StreamingAsyncConsumer(org.apache.kafka.clients.consumer.Consumer<K, V> consumer,
                                  Producer<K, V> producer,
                                  AsyncConsumerOptions asyncConsumerOptions) {
        super(consumer, producer, asyncConsumerOptions);

        userProcessResultsStream = new ConcurrentLinkedDeque<>();

        stream = Java8StreamUtils.setupStreamFromDeque(userProcessResultsStream);
    }

    /**
     * Like {@link AsyncConsumer#asyncPollAndProduce} but instead of callbacks, streams the results instead, after the
     * produce result is ack'd by Kafka.
     *
     * @return a stream of results of applying the function to the polled records
     */
    public Stream<ConsumeProduceResult<K, V, K, V>> asyncPollProduceAndStream(Function<ConsumerRecord<K, V>, List<ProducerRecord<K, V>>> userFunction) {
        super.asyncPollAndProduce(userFunction, (result) -> {
            log.trace("Wrapper callback applied, sending result to stream. Input: {}", result);
            userProcessResultsStream.add(result);
        });

        return stream;
    }

}
