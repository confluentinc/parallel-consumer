package io.confluent.parallelconsumer;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public interface PCStream<K, V> {
    void map(V o);

    void foreach(V o);

    void join(V table, V o);
}
