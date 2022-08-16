package io.confluent.parallelconsumer;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public interface PCStream<K, V> {

    PCStream map(RecordProcessor.Transformer o);

    PCStream map(KeyValueMapper mapper);

    <KR, VR> KStream<KR, VR> flatMap(final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper);


    void foreach(RecordProcessor.Processor o);

    void join(V table, V o);

    void to(String s);
}
