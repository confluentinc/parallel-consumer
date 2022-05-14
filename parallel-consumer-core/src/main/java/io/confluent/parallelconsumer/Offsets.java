package io.confluent.parallelconsumer;

import lombok.ToString;
import lombok.experimental.Delegate;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@ToString
public class Offsets {

    @Delegate
    private final List<Long> rawOffsets;

    public Offsets(List<Long> records) {
        this.rawOffsets = records;
    }

    public static Offsets from(List<RecordContext<?, ?>> records) {
        return of(records.stream()
                .map(RecordContext::offset)
                .collect(Collectors.toUnmodifiableList()));
    }

    // due to type erasure, can't use method overloading
    public static Offsets of(List<Long> rawOffsetsIn) {
        return new Offsets(rawOffsetsIn);
    }

    public static Offsets of(long... rawOffsetsIn) {
        return new Offsets(Arrays.stream(rawOffsetsIn).boxed().collect(Collectors.toList()));
    }
}
