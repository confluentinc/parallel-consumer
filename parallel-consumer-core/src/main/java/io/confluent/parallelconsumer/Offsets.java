package io.confluent.parallelconsumer;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class Offsets {

    @Delegate
    private final List<Long> rawOffsets;

    public Offsets(List<RecordContext<?, ?>> records) {
        rawOffsets = records.stream()
                .map(RecordContext::offset)
                .collect(Collectors.toUnmodifiableList());
    }
}
