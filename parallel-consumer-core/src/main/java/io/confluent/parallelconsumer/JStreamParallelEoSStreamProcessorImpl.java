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
public class JStreamParallelEoSStreamProcessorImpl<K, V> extends ParallelEoSStreamProcessorImpl<K, V> {

    private final Stream<ConsumeProduceResult<K, V, K, V>> stream;

    private final ConcurrentLinkedDeque<ConsumeProduceResult<K, V, K, V>> userProcessResultsStream;

    public JStreamParallelEoSStreamProcessorImpl(org.apache.kafka.clients.consumer.Consumer<K, V> consumer,
                                                 Producer<K, V> producer,
                                                 ParallelConsumerOptions parallelConsumerOptions) {
        super(consumer, producer, parallelConsumerOptions);

        this.userProcessResultsStream = new ConcurrentLinkedDeque<>();

        this.stream = Java8StreamUtils.setupStreamFromDeque(this.userProcessResultsStream);
    }

    /**
     * Like {@link ParallelEoSStreamProcessorImpl#pollAndProduce} but instead of callbacks, streams the results instead, after the
     * produce result is ack'd by Kafka.
     *
     * @return a stream of results of applying the function to the polled records
     */
    public Stream<ConsumeProduceResult<K, V, K, V>> pollProduceAndStream(Function<ConsumerRecord<K, V>, List<ProducerRecord<K, V>>> userFunction) {
        super.pollAndProduce(userFunction, (result) -> {
            log.trace("Wrapper callback applied, sending result to stream. Input: {}", result);
            this.userProcessResultsStream.add(result);
        });

        return this.stream;
    }

}
