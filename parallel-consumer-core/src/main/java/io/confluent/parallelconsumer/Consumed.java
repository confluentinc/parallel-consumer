/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.parallelconsumer;

import lombok.AllArgsConstructor;
import org.apache.kafka.common.serialization.Serde;

/**
 * The {@code Consumed} class is used to define the optional parameters when using {@link StreamsBuilder} to build
 * instances of {@link KStream}, {@link KTable}, and {@link GlobalKTable}.
 * <p>
 * For example, you can read a topic as {@link KStream} with a custom timestamp extractor and specify the corresponding
 * key and value serdes like:
 * <pre>{@code
 * StreamsBuilder builder = new StreamsBuilder();
 * KStream<String, Long> stream = builder.stream(
 *   "topicName",
 *   Consumed.with(Serdes.String(), Serdes.Long())
 *           .withTimestampExtractor(new LogAndSkipOnInvalidTimestamp()));
 * }</pre>
 * Similarly, you can read a topic as {@link KTable} with a custom {@code auto.offset.reset} configuration and force a
 * state store {@link org.apache.kafka.streams.kstream.Materialized materialization} to access the content via
 * interactive queries:
 * <pre>{@code
 * StreamsBuilder builder = new StreamsBuilder();
 * KTable<Integer, Integer> table = builder.table(
 *   "topicName",
 *   Consumed.with(AutoOffsetReset.LATEST),
 *   Materialized.as("queryable-store-name"));
 * }</pre>
 *
 * @param <K> type of record key
 * @param <V> type of record value
 */
@AllArgsConstructor
public class Consumed<K, V> { //implements NamedOperation<Consumed<K, V>> {

    protected Serde<K> keySerde;
    protected Serde<V> valueSerde;
//    protected TimestampExtractor timestampExtractor;
//    protected Topology.AutoOffsetReset resetPolicy;
//    protected String processorName;

//    /**
//     * Create an instance of {@link Consumed} with the supplied arguments. {@code null} values are acceptable.
//     *
//     * @param keySerde           the key serde. If {@code null} the default key serde from config will be used
//     * @param valueSerde         the value serde. If {@code null} the default value serde from config will be used
//     * @param timestampExtractor the timestamp extractor to used. If {@code null} the default timestamp extractor from config will be used
//     * @param resetPolicy        the offset reset policy to be used. If {@code null} the default reset policy from config will be used
//     * @param <K>                key type
//     * @param <V>                value type
//     * @return a new instance of {@link Consumed}
//     */
//    public static <K, V> Consumed<K, V> with(final Serde<K> keySerde,
//                                             final Serde<V> valueSerde,
//                                             final TimestampExtractor timestampExtractor,
//                                             final Topology.AutoOffsetReset resetPolicy) {
//        return new Consumed<>(keySerde, valueSerde, timestampExtractor, resetPolicy, null);
//
//    }

    /**
     * Create an instance of {@link Consumed} with key and value {@link Serde}s.
     *
     * @param keySerde   the key serde. If {@code null} the default key serde from config will be used
     * @param valueSerde the value serde. If {@code null} the default value serde from config will be used
     * @param <K>        key type
     * @param <V>        value type
     * @return a new instance of {@link Consumed}
     */
    public static <K, V> Consumed<K, V> with(final Serde<K> keySerde,
                                             final Serde<V> valueSerde) {
//        return new Consumed<>(keySerde, valueSerde, null, null, null);
        return new Consumed<>(keySerde, valueSerde);
    }

//    /**
//     * Create an instance of {@link Consumed} with a {@link TimestampExtractor}.
//     *
//     * @param timestampExtractor the timestamp extractor to used. If {@code null} the default timestamp extractor from config will be used
//     * @param <K>                key type
//     * @param <V>                value type
//     * @return a new instance of {@link Consumed}
//     */
//    public static <K, V> Consumed<K, V> with(final TimestampExtractor timestampExtractor) {
//        return new Consumed<>(null, null, timestampExtractor, null, null);
//    }
//
//    /**
//     * Create an instance of {@link Consumed} with a {@link org.apache.kafka.streams.Topology.AutoOffsetReset Topology.AutoOffsetReset}.
//     *
//     * @param resetPolicy the offset reset policy to be used. If {@code null} the default reset policy from config will be used
//     * @param <K>         key type
//     * @param <V>         value type
//     * @return a new instance of {@link Consumed}
//     */
//    public static <K, V> Consumed<K, V> with(final Topology.AutoOffsetReset resetPolicy) {
//        return new Consumed<>(null, null, null, resetPolicy, null);
//    }

//    /**
//     * Create an instance of {@link Consumed} with provided processor name.
//     *
//     * @param processorName the processor name to be used. If {@code null} a default processor name will be generated
//     * @param <K>         key type
//     * @param <V>         value type
//     * @return a new instance of {@link Consumed}
//     */
//    public static <K, V> Consumed<K, V> as(final String processorName) {
//        return new Consumed<>(null, null, null, null, processorName);
//    }

    /**
     * Configure the instance of {@link Consumed} with a key {@link Serde}.
     *
     * @param keySerde the key serde. If {@code null}the default key serde from config will be used
     * @return this
     */
    public Consumed<K, V> withKeySerde(final Serde<K> keySerde) {
        this.keySerde = keySerde;
        return this;
    }

    /**
     * Configure the instance of {@link Consumed} with a value {@link Serde}.
     *
     * @param valueSerde the value serde. If {@code null} the default value serde from config will be used
     * @return this
     */
    public Consumed<K, V> withValueSerde(final Serde<V> valueSerde) {
        this.valueSerde = valueSerde;
        return this;
    }

//    /**
//     * Configure the instance of {@link Consumed} with a {@link TimestampExtractor}.
//     *
//     * @param timestampExtractor the timestamp extractor to used. If {@code null} the default timestamp extractor from config will be used
//     * @return this
//     */
//    public Consumed<K, V> withTimestampExtractor(final TimestampExtractor timestampExtractor) {
//        this.timestampExtractor = timestampExtractor;
//        return this;
//    }
//
//    /**
//     * Configure the instance of {@link Consumed} with a {@link org.apache.kafka.streams.Topology.AutoOffsetReset Topology.AutoOffsetReset}.
//     *
//     * @param resetPolicy the offset reset policy to be used. If {@code null} the default reset policy from config will be used
//     * @return this
//     */
//    public Consumed<K, V> withOffsetResetPolicy(final Topology.AutoOffsetReset resetPolicy) {
//        this.resetPolicy = resetPolicy;
//        return this;
//    }

//
//    /**
//     * Configure the instance of {@link Consumed} with a processor name.
//     *
//     * @param processorName the processor name to be used. If {@code null} a default processor name will be generated
//     * @return this
//     */
//    @Override
//    public Consumed<K, V> withName(final String processorName) {
//        this.processorName = processorName;
//        return this;
//    }
//
}
