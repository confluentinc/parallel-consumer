package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.micrometer.core.instrument.Tags;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.SuperBuilder;

import java.time.Duration;

@Value
@SuperBuilder(toBuilder = true)
public class MetricsEvent {


    public enum MetricsType { COUNTER, TIMER, GAUGE }
    String name;
    String description;
    Double value;

    Duration timerValue;
    String unit;
    @Builder.Default
    Tags tags = Tags.empty();
    @Builder.Default
    MetricsType type = MetricsType.COUNTER;
}
