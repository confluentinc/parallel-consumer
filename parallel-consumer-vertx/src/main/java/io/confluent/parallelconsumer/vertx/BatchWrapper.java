package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.Data;

import java.util.List;

@Data
class BatchWrapper<K, V> {
    private final List<WorkContainer<K, V>> batch;
}
