package io.confluent.parallelconsumer;

import java.util.Collection;
import java.util.regex.Pattern;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public interface PCTopologyBuilder {

    <K, V> PCStream<K, V> stream(String topicName);

    <K, V> PCStream<K, V> stream(String topicName, Consumed<K, V> consumed);

    <K, V> PCStream<K, V> stream(Collection<String> topics);

    <K, V> PCStream<K, V> stream(Collection<String> topics, Consumed<K, V> consumed);

    <K, V> PCStream<K, V> stream(Pattern topicPattern);

    <K, V> PCStream<K, V> stream(Pattern topicPattern, Consumed<K, V> consumed);


    PCTopolgy build();
}
