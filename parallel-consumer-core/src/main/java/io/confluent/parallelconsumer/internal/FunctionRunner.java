package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.state.WorkContainer;

import java.util.function.Consumer;

public class FunctionRunner<K, V> {
    public void run(Consumer<Void> callback, WorkContainer<K, V> work) {
    }
}
