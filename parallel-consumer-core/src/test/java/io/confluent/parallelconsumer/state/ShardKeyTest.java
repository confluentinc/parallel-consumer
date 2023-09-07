package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Objects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Antony Stubbs
 * @see ShardKey
 */
class ShardKeyTest {

    /**
     * Tests when KEY ordering with `null` keyed records
     */
    @Test
    void nullKey() {
        var cr = mock(ConsumerRecord.class);
        when(cr.partition()).thenReturn(0);
        when(cr.topic()).thenReturn("atopic");
        when(cr.key()).thenReturn(null);

        var wc = mock(WorkContainer.class);
        when(wc.getCr()).thenReturn(cr);

        ShardKey.of(wc, KEY);
    }

    // todo split up
    @Test
    void keyTest() {
        ParallelConsumerOptions.ProcessingOrder ordering = KEY;
        String topicOne = "t1";
        TopicPartition topicOneP0 = new TopicPartition("t1", 0);
        String keyOne = "k1";
        String keyOneAgain = "k1";

        // same inputs, different key instances equal
        var reck1 = new ConsumerRecord<>(topicOne, 0, 0, keyOne, "v");
        var reck1Again = new ConsumerRecord<>(topicOne, 0, 0, keyOneAgain, "v");

        ShardKey key1 = ShardKey.of(reck1, ordering);
        ShardKey anotherInstanceWithSameInputs = ShardKey.of(reck1Again, ordering);
        assertThat(key1).isEqualTo(anotherInstanceWithSameInputs);

        // same topic, same partition, different key
        var reck2 = new ConsumerRecord<>(topicOne, 0, 0, "k2", "v");
        ShardKey of3 = ShardKey.of(reck2, ordering);
        assertThat(key1).isNotEqualTo(of3);

        // different topic, same key
        var reck3 = new ConsumerRecord<>("t2", 0, 0, keyOne, "v");
        assertThat(key1).isNotEqualTo(ShardKey.of(reck3, ordering));

        // same topic, same key
        ShardKey.KeyOrderedKey keyOrderedKey = new ShardKey.KeyOrderedKey(topicOneP0, keyOne);
        ShardKey.KeyOrderedKey keyOrderedKeyTwo = new ShardKey.KeyOrderedKey(topicOneP0, keyOne);
        assertThat(keyOrderedKey).isEqualTo(keyOrderedKeyTwo);

        // same topic, same key, different partition
        var reck4 = new ConsumerRecord<>(topicOne, 1, 0, keyOne, "v");
        ShardKey of4 = ShardKey.of(reck2, ordering);
        assertThat(key1).isNotEqualTo(of3);

        // check both exist in queue too ??
        //assertThat("false").isEmpty();
    }

    private static Object keyObject = new Object();

    static Stream<Arguments> keyEqualityParams() {
        return Stream.of(
                Arguments.of(keyObject, keyObject),
                Arguments.of("key", "key"),
                Arguments.of((byte) 1, (byte) 1),
                Arguments.of(true, true),
                Arguments.of((short) 1, (short) 1),
                Arguments.of(1, 1),
                Arguments.of(1L, 1L),
                Arguments.of((float) 1.1, (float) 1.1),
                Arguments.of(1.1, 1.1),
                Arguments.of('a', 'a'),
                Arguments.of(null, null),
                Arguments.of(Boolean.TRUE, Boolean.TRUE),
                Arguments.of(Short.valueOf((short) 1), Short.valueOf((short) 1)),
                Arguments.of(Integer.valueOf(1), Integer.valueOf(1)),
                Arguments.of(Long.valueOf(1L), Long.valueOf(1L)),
                Arguments.of(Float.valueOf((float) 1.1), Float.valueOf((float) 1.1)),
                Arguments.of(Double.valueOf(1.1), Double.valueOf(1.1)),
                Arguments.of(Character.valueOf('a'), Character.valueOf('a')),
                Arguments.of(null, null),

                Arguments.of(new Object[]{keyObject, "key"}, new Object[]{keyObject, "key"}),
                Arguments.of(new String[]{"key1", "key2"}, new String[]{"key1", "key2"}),
                Arguments.of(new byte[]{1, 2}, new byte[]{1, 2}),
                Arguments.of(new boolean[]{true, false}, new boolean[]{true, false}),
                Arguments.of(new short[]{1, 2}, new short[]{1, 2}),
                Arguments.of(new int[]{1, 2}, new int[]{1, 2}),
                Arguments.of(new long[]{1, 2}, new long[]{1, 2}),
                Arguments.of(new float[]{1, 2}, new float[]{1, 2}),
                Arguments.of(new double[]{1, 2}, new double[]{1, 2}),
                Arguments.of(new char[]{'1', '2'}, new char[]{'1', '2'}),
                Arguments.of(new Object[]{null}, new Object[]{null})
                );
    }

    /**
     * Parametrized key equality test for different reference and primitive types including arrays.
     */
    @ParameterizedTest
    @MethodSource("keyEqualityParams")
    void testKeyEquality(Object keyOne, Object keyTwo) {
        String topic = "topic1";
        var reck1 = new ConsumerRecord<>(topic, 0, 0, keyOne, "v");
        var reck2 = new ConsumerRecord<>(topic, 0, 0, keyTwo, "v");

        ShardKey shardKey1 = ShardKey.of(reck1, KEY);
        ShardKey shardKey2 = ShardKey.of(reck2, KEY);
        assertThat(shardKey1).isEqualTo(shardKey2);
    }

    /**
     * Tests that equality works correctly for byte[] keys - based on array contents not ref.
     */
    @Test
    void keyTestByteArray() {
        ParallelConsumerOptions.ProcessingOrder ordering = KEY;
        String topicOne = "t1";
        byte[] keyOne = "k1".getBytes();
        byte[] keyOneAgain = "k1".getBytes();

        // same inputs, different key instances equal
        var reck1 = new ConsumerRecord<>(topicOne, 0, 0, keyOne, "v");
        var reck1Again = new ConsumerRecord<>(topicOne, 0, 0, keyOneAgain, "v");

        ShardKey key1 = ShardKey.of(reck1, ordering);
        ShardKey anotherInstanceWithSameInputs = ShardKey.of(reck1Again, ordering);
        assertThat(key1).isEqualTo(anotherInstanceWithSameInputs);
    }
}
