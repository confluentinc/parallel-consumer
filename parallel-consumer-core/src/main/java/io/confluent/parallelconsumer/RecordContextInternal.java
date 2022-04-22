package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.controller.WorkContainer;
import lombok.Getter;

/**
 * Internal only view of the {@link RecordContext} class.
 */
public class RecordContextInternal<K, V> {

    @Getter
    private final RecordContext<K, V> recordContext;

    public RecordContextInternal(WorkContainer<K, V> wc) {
        this.recordContext = new RecordContext<>(wc);
    }

    public WorkContainer<K, V> getWorkContainer() {
        return getRecordContext().getWorkContainer();
    }
}
