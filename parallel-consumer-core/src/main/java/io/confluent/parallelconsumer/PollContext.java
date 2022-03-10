package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TODO docs
 */
@AllArgsConstructor
@Value(staticConstructor = "of")
@Getter(AccessLevel.NONE)
//@Setter(AccessLevel.NONE)
public class PollContext<K, V> implements Iterable<RecordContext<K, V>> {

    Map<TopicPartition, Set<RecordContext<K, V>>> records = new HashMap<>();

    public PollContext(List<WorkContainer<K, V>> workContainers) {
        for (var wc : workContainers) {
            TopicPartition topicPartition = wc.getTopicPartition();
            var recordSet = records.computeIfAbsent(topicPartition, ignore -> new HashSet<>());
            recordSet.add(new RecordContext<>(wc));
        }
    }

    /**
     * Not public
     */
    Stream<WorkContainer<K, V>> streamWorkContainers() {
        return stream().map(RecordContext::getWorkContainer);
    }

    public Stream<ConsumerRecord<K, V>> streamConsumerRecords() {
        return stream().map(RecordContext::getConsumerRecord);
    }

    public Stream<RecordContext<K, V>> stream() {
        return getByTopicPartitionMap().values().stream().flatMap(Collection::stream);
    }

    /**
     * Throws {@link IllegalStateException} if a {@link ParallelConsumerOptions#getBatchSize()} has been set.
     *
     * @return the single record entry in this context
     */
    public RecordContext<K, V> getSingleRecord() {
        if (size() != 1) {
            // todo docs - same message as before
            throw new IllegalArgumentException("TODO");
        }
        //noinspection OptionalGetWithoutIsPresent
        return stream().findFirst().get(); // NOSONAR
    }

    public ConsumerRecord<K, V> getSingleConsumerRecord() {
        return getSingleRecord().getConsumerRecord();
    }

    /**
     * For backwards compatiabilty with {@link ConsumerRecord#value()}
     */
    public V value() {
        return getSingleConsumerRecord().value();
    }

    /**
     * For backwards compatiabilty with {@link ConsumerRecord#value()}
     */
    public K key() {
        return getSingleConsumerRecord().key();
    }

    /**
     * For backwards compatiabilty with {@link ConsumerRecord#value()}
     */
    public long offset() {
        return getSingleConsumerRecord().offset();
    }

    public List<RecordContext<K, V>> getContextsFlattened() {
        return records.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    public List<ConsumerRecord<K, V>> getConsumerRecordsFlattened() {
        return streamConsumerRecords().collect(Collectors.toList());
    }

    // todo package private - better move to separate class
    public List<WorkContainer<K, V>> getWorkContainers() {
        return streamWorkContainers().collect(Collectors.toList());
    }

    @Override
    public Iterator<RecordContext<K, V>> iterator() {
        return stream().iterator();
    }

    @Override
    public void forEach(Consumer<? super RecordContext<K, V>> action) {
        Iterable.super.forEach(action);
    }

    @Override
    public Spliterator<RecordContext<K, V>> spliterator() {
        return Iterable.super.spliterator();
    }

    public Map<TopicPartition, Set<RecordContext<K, V>>> getByTopicPartitionMap() {
        return Collections.unmodifiableMap(this.records);
    }

    public long size() {
        return stream().count();
    }

    public List<Long> getOffsets() {
        return streamConsumerRecords().mapToLong(ConsumerRecord::offset).boxed().collect(Collectors.toList());
    }
}
