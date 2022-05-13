package io.confluent.parallelconsumer;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

import java.util.List;

@RequiredArgsConstructor
public class Offsets {
    @Delegate
    final private List<Long> ofssets;
}
