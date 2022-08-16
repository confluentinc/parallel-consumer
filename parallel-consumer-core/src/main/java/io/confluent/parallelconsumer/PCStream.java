package io.confluent.parallelconsumer;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public interface PCStream<K, V> {
    void map(RecordProcessor.PollConsumerAndProducer o);

    void foreach(RecordProcessor.PollConsumer o);

    void join(V table, V o);
}
