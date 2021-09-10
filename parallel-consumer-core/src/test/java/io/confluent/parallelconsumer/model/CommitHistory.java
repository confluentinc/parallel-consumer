package io.confluent.parallelconsumer.model;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.List;
import java.util.stream.Collectors;

public class CommitHistory {

    private final List<OffsetAndMetadata> history;

    public CommitHistory(final List<OffsetAndMetadata> collect) {
        super();
        this.history = collect;
    }

    public boolean contains(final int offset) {
        return history.stream().anyMatch(x -> x.offset() == offset);
    }

    public long highestCommit() {
        return history.get(history.size() - 1).offset();
    }

    public List<Long> getOffsetHistory() {
        return history.stream().map(OffsetAndMetadata::offset).collect(Collectors.toList());
    }
}
