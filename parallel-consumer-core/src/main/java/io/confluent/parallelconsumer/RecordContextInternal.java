package io.confluent.parallelconsumer;

import io.confluent.parallelconsumer.state.WorkContainer;

/**
 * Internal only view of the {@link RecordContext} class.
 * todo
 */
public class RecordContextInternal<K, V> extends RecordContext<K, V> {

    public RecordContextInternal(WorkContainer<K, V> wc) {
        super(wc);
    }

    public WorkContainer<K, V> getWorkContainer() {
        return super.workContainer;
    }
}
