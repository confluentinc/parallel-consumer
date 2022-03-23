package io.confluent.parallelconsumer;

import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.Getter;
import lombok.experimental.Delegate;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Internal only view on the {@link PollContext}.
 * <p>
 * Not public - not part of user API.
 * <p>
 * NB: Yes, a user could cast to the {@link PollContextInternal} class to get access to other public APIs, but they can
 * do lots of things to work around the structure that keeps internals internal.
 */
public class PollContextInternal<K, V> {

    @Delegate
    @Getter
    private final PollContext<K, V> pollContext;

    public PollContextInternal(List<WorkContainer<K, V>> workContainers) {
        this.pollContext = new PollContext<>(workContainers);
    }

    /**
     * Not public - not part of user API
     */
    public Stream<WorkContainer<K, V>> streamWorkContainers() {
        return pollContext.streamInternal().map(RecordContextInternal::getWorkContainer);
    }

    /**
     * This MUST NOT be public. {@link WorkContainer} is not part of the public API, and is mutable.
     *
     * @return a flat {@link List} of {@link WorkContainer}s, which wrap the {@link ConsumerRecord}s in this result set
     */
    public List<WorkContainer<K, V>> getWorkContainers() {
        return streamWorkContainers().collect(Collectors.toList());
    }


}
