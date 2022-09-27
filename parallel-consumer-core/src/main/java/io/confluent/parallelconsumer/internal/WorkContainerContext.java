package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import lombok.Value;

import java.time.Clock;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
@Value
public class WorkContainerContext<K, V> {
    ParallelConsumerOptions<K, V> options;
    Clock clock;
}
