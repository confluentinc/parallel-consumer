package io.confluent.parallelconsumer.model;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.CollectionUtils;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.List;
import java.util.Optional;
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

    public Optional<Long> highestCommit() {
        Optional<OffsetAndMetadata> last = CollectionUtils.getLast(history);
        return last.map(OffsetAndMetadata::offset);
    }

    public List<Long> getOffsetHistory() {
        return history.stream().map(OffsetAndMetadata::offset).collect(Collectors.toList());
    }

    @SneakyThrows
    public HighestOffsetAndIncompletes getEncodedSucceeded() {
        Optional<OffsetAndMetadata> first = getHead();
        OffsetAndMetadata offsetAndMetadata = first.get();
        HighestOffsetAndIncompletes highestOffsetAndIncompletes =
                OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(offsetAndMetadata.offset(), offsetAndMetadata.metadata());
        return highestOffsetAndIncompletes;
    }

    @NonNull
    private Optional<OffsetAndMetadata> getHead() {
        Optional<OffsetAndMetadata> first = history.isEmpty()
                ? Optional.empty()
                : Optional.of(history.get(history.size() - 1));
        return first;
    }

    public String getEncoding() {
        return getHead().get().metadata();
    }
}
