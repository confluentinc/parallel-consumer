package io.confluent.parallelconsumer;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public interface PCStream<K, V> {

    PCStream<K, V> map(RecordProcessor.Transformer<K, V> o);

    <VR> PCStream<K, VR> map(KeyValueMapper<K, V, VR> mapper);

    <KR, VR> PCStream<KR, VR> flatMap(KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper);

    void foreach(RecordProcessor.Processor<K, V> o);

    void join(V table, V o);

    void to(String s);

    PCStream<K, V> through(String s);
}
