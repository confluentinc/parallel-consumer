package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.Getter;
import lombok.experimental.Delegate;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Internal only view on the {@link PollContext}.
 */
public class PollContextInternal<K, V> {

    @Delegate
    @Getter
    private final PollContext<K, V> pollContext;

    public PollContextInternal(List<WorkContainer<K, V>> workContainers) {
        this.pollContext = new PollContext<>(workContainers);
    }

    /**
     * @return a stream of {@link WorkContainer}s
     */
    public Stream<WorkContainer<K, V>> streamWorkContainers() {
        return pollContext.streamInternal().map(RecordContextInternal::getWorkContainer);
    }

    /**
     * @return a flat {@link List} of {@link WorkContainer}s, which wrap the {@link ConsumerRecord}s in this result set
     */
    public List<WorkContainer<K, V>> getWorkContainers() {
        return streamWorkContainers().collect(Collectors.toList());
    }


}
