package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.Consumed;
import io.confluent.parallelconsumer.PCStream;
import io.confluent.parallelconsumer.PCTopolgy;
import io.confluent.parallelconsumer.PCTopologyBuilder;
import org.apache.kafka.common.serialization.Serde;

import java.util.Collection;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public class PCTopologyBuilderImpl implements PCTopologyBuilder {

    private final Optional<Serde<?>> defaultConsumeSerde = Optional.empty();
    private final Optional<Serde<?>> defaultProduceSerde = Optional.empty();

    public PCTopologyBuilderImpl() {
        this.defaultConsumeSerde = Optional.empty();
        this.defaultProduceSerde = Optional.empty();
    }

    public PCTopologyBuilderImpl(Serde<String> serde) {
        this.defaultConsumeSerde = Optional.of(serde);
        this.defaultProduceSerde = Optional.of(serde);
    }

    public PCTopologyBuilderImpl(Serde<String> consumeSerde, Serde<Long> produceSerde) {
        this.defaultConsumeSerde = Optional.of(consumeSerde);
        this.defaultProduceSerde = Optional.of(produceSerde);
    }

    @Override
    public <K, V> PCStream<K, V> stream(final String topicName) {
        return null;
    }

    @Override
    public <K, V> PCStream<K, V> stream(final String topicName, final Consumed<K, V> consumed) {
        return null;
    }

    @Override
    public <K, V> PCStream<K, V> stream(final Collection<String> topics) {
        return null;
    }

    @Override
    public <K, V> PCStream<K, V> stream(final Collection<String> topics, final Consumed<K, V> consumed) {
        return null;
    }

    @Override
    public <K, V> PCStream<K, V> stream(final Pattern topicPattern) {
        return null;
    }

    @Override
    public <K, V> PCStream<K, V> stream(final Pattern topicPattern, final Consumed<K, V> consumed) {
        return null;
    }

    @Override
    public PCTopolgy build() {
        return null;
    }
}
