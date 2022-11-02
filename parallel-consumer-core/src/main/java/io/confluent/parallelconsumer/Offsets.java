package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.Getter;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Delegate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.stream.Collectors;

/**
 * A range of {@link Offset}s.
 */
@ToString
public class Offsets {

    @Getter
    @Delegate
    private final List<Long> rawOffsets;

    public Offsets(List<Long> records) {
        this.rawOffsets = records;
    }

    public static Offsets fromRecords(List<RecordContext<?, ?>> records) {
        return fromLongs(records.stream()
                .map(RecordContext::offset)
                .collect(Collectors.toList()));
    }

    // due to type erasure, can't use method overloading
    public static Offsets fromLongs(List<Long> rawOffsetsIn) {
        return new Offsets(rawOffsetsIn);
    }

    public static Offsets fromArray(long... rawOffsetsIn) {
        return new Offsets(Arrays.stream(rawOffsetsIn).boxed().collect(Collectors.toList()));
    }

    public static Offsets from(SortedSet<Long> incompleteOffsetsBelowHighestSucceeded) {
        return new Offsets(new ArrayList<>(incompleteOffsetsBelowHighestSucceeded));
    }

    /**
     * Class value for a Kafka offset, to avoid being Longly typed.
     */
    @Value
    public static class Offset {
        long value;

        public static Offset of(Long offset) {
            return new Offset(offset);
        }
    }
}
