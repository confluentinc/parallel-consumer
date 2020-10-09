package io.confluent.parallelconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public interface JStreamParallelStreamProcessor<K, V> extends DrainingCloseable {

    static <KK, VV> JStreamParallelStreamProcessor<KK, VV> createJStreamEosStreamProcessor(
            org.apache.kafka.clients.consumer.Consumer<KK, VV> consumer,
            org.apache.kafka.clients.producer.Producer<KK, VV> producer,
            ParallelConsumerOptions options) {
        return new JStreamParallelEoSStreamProcessor<>(consumer, producer, options);
    }

    /**
     * Like {@link ParallelEoSStreamProcessor#pollAndProduce} but instead of callbacks, streams the results instead,
     * after the produce result is ack'd by Kafka.
     *
     * @return a stream of results of applying the function to the polled records
     */
    Stream<ParallelStreamProcessor.ConsumeProduceResult<K, V, K, V>> pollProduceAndStream(Function<ConsumerRecord<K, V>,
            List<ProducerRecord<K, V>>> userFunction);
}
