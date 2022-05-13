package io.confluent.parallelconsumer;

import lombok.experimental.Delegate;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Offsets {

    @Delegate
    private final List<Long> rawOffsets;

    public Offsets(List<Long> records) {
        this.rawOffsets = records;
    }

    public static Offsets of(List<RecordContext<?, ?>> records) {
        return ofLongs(records.stream()
                .map(RecordContext::offset)
                .collect(Collectors.toUnmodifiableList()));
    }

    // due to type erasure, can't use method overloading
    public static Offsets ofLongs(List<Long> rawOffsetsIn) {
        return new Offsets(rawOffsetsIn);
    }

    public static Offsets ofLongs(long... rawOffsetsIn) {
        return new Offsets(Arrays.stream(rawOffsetsIn).boxed().collect(Collectors.toList()));
    }
}
