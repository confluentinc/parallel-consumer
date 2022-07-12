package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.state.WorkContainer;

import java.util.Optional;

public interface ControllerAPI<K, V> {

    void sendNewPolledRecordsAsync(EpochAndRecordsMap<K, V> polledRecords);

    void sendWorkResultAsync(WorkContainer<K, V> wc);

    Optional<Object> getMyId();
}
