package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.csid.utils.Java8StreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Function;
import java.util.stream.Stream;

@Slf4j
public class JStreamParallelEoSStreamProcessor<K, V> extends ParallelEoSStreamProcessor<K, V> implements JStreamParallelStreamProcessor<K, V> {

    private final Stream<ConsumeProduceResult<K, V, K, V>> stream;

    private final ConcurrentLinkedDeque<ConsumeProduceResult<K, V, K, V>> userProcessResultsStream;

    public JStreamParallelEoSStreamProcessor(ParallelConsumerOptions parallelConsumerOptions) {
        super(parallelConsumerOptions);

        this.userProcessResultsStream = new ConcurrentLinkedDeque<>();

        this.stream = Java8StreamUtils.setupStreamFromDeque(this.userProcessResultsStream);
    }

    @Override
    public Stream<ConsumeProduceResult<K, V, K, V>> pollProduceAndStream(Function<ConsumerRecord<K, V>, List<ProducerRecord<K, V>>> userFunction) {
        super.pollAndProduceMany(userFunction, (result) -> {
            log.trace("Wrapper callback applied, sending result to stream. Input: {}", result);
            this.userProcessResultsStream.add(result);
        });

        return this.stream;
    }

}
