package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.state.WorkContainer;

import java.util.Optional;

public class ControllerAPI<K, V> {

    public void sendNewPolledRecordsAsync(EpochAndRecordsMap<K, V> polledRecords) {

    }

    protected void sendWorkResultAsync(WorkContainer<K, V> wc) {
    }


    public Optional<Object> getMyId() {
        return null;
    }
}
