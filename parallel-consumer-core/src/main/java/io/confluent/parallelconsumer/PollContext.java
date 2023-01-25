package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.internal.Documentation.getLinkHtmlToDocSection;


/**
 * Context object used to pass messages to process to users processing functions.
 * <p>
 * Results sets can be iterated in a variety of ways. Explore the different methods available.
 * <p>
 * You can access for {@link ConsumerRecord}s directly, or you can get the {@link RecordContext} wrappers, which provide
 * extra information about the specific records, such as {@link RecordContext#getNumberOfFailedAttempts()}.
 * <p>
 * Note that if you are not setting a {@link ParallelConsumerOptions#batchSize}, then you can use the {@link
 * #getSingleRecord()}, and it's convenience accessors ({@link #value()}, {@link #offset()}, {@link #key()} {@link
 * #getSingleConsumerRecord()}). But if you have configured batching, they will all throw an {@link
 * IllegalArgumentException}, as it's not valid to have batches of messages and yet tread the batch input as a single
 * record.
 */
@AllArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@ToString
@EqualsAndHashCode
public class PollContext<K, V> implements Iterable<RecordContext<K, V>> {

    protected Map<TopicPartition, Set<RecordContextInternal<K, V>>> records = new HashMap<>();

    PollContext(List<WorkContainer<K, V>> workContainers) {
        for (var wc : workContainers) {
            TopicPartition topicPartition = wc.getTopicPartition();
            var recordSet = records.computeIfAbsent(topicPartition, ignore -> new HashSet<>());
            recordSet.add(new RecordContextInternal<>(wc));
        }
    }

    /**
     * @return a flat {@link Stream} of {@link RecordContext}s, which wrap the {@link ConsumerRecord}s in this result
     * set
     */
    public Stream<RecordContextInternal<K, V>> streamInternal() {
        return this.records.values().stream().flatMap(Collection::stream);
    }

    /**
     * @return a flat {@link Stream} of {@link RecordContext}s, which wrap the {@link ConsumerRecord}s in this result
     * set
     */
    public Stream<RecordContext<K, V>> stream() {
        return getByTopicPartitionMap().values().stream().flatMap(Collection::stream);
    }

    /**
     * @return a flat {@link Stream} of {@link ConsumerRecord} in this poll set
     */
    public Stream<ConsumerRecord<K, V>> streamConsumerRecords() {
        return stream().map(RecordContext::getConsumerRecord);
    }

    /**
     * Must not be using batching ({@link ParallelConsumerOptions#batchSize}).
     *
     * @return the single {@link RecordContext} entry in this poll set
     * @throws IllegalArgumentException if a {@link ParallelConsumerOptions#getBatchSize()} has been set.
     */
    public RecordContext<K, V> getSingleRecord() {
        // instead of calling Options#isUsingBatch - this way we don't need to access to the options class, and this is effectively the same thing
        if (size() != 1) {
            throw new IllegalArgumentException(msg("A 'batch size' has been specified in `options`, so you must use the `batch` versions of the polling methods. See {}", getLinkHtmlToDocSection("#batching")));
        }
        //noinspection OptionalGetWithoutIsPresent
        return stream().findFirst().get(); // NOSONAR
    }

    /**
     * Must not be using batching ({@link ParallelConsumerOptions#batchSize}).
     *
     * @return the single {@link ConsumerRecord} entry in this poll set
     * @see #getSingleRecord()
     */
    public ConsumerRecord<K, V> getSingleConsumerRecord() {
        return getSingleRecord().getConsumerRecord();
    }

    /**
     * For backwards compatibility with {@link ConsumerRecord#value()}.
     * <p>
     * Must not be using batching ({@link ParallelConsumerOptions#batchSize}).
     *
     * @return the single {@link ConsumerRecord#value()} entry in this poll set
     * @see #getSingleRecord()
     */
    public V value() {
        return getSingleConsumerRecord().value();
    }

    /**
     * For backwards compatibility with {@link ConsumerRecord#key()}.
     * <p>
     * Must not be using batching ({@link ParallelConsumerOptions#batchSize}).
     *
     * @return the single {@link ConsumerRecord#key()} entry in this poll set
     * @see #getSingleRecord()
     */
    public K key() {
        return getSingleConsumerRecord().key();
    }

    /**
     * For backwards compatibility with {@link ConsumerRecord#offset()}.
     * <p>
     * Must not be using batching ({@link ParallelConsumerOptions#batchSize}).
     *
     * @return the single {@link ConsumerRecord#offset()} entry in this poll set
     * @see #getSingleRecord()
     */
    public long offset() {
        return getSingleConsumerRecord().offset();
    }

    /**
     * @return a flat {@link List} of {@link RecordContext}s, which wrap the {@link ConsumerRecord}s in this result set
     */
    public List<RecordContext<K, V>> getContextsFlattened() {
        return records.values().stream()
                .flatMap(Collection::stream)
                .map(RecordContextInternal::getRecordContext)
                .collect(Collectors.toList());
    }

    /**
     * @return a flat {@link List} of {@link ConsumerRecord}s in this result set
     */
    public List<ConsumerRecord<K, V>> getConsumerRecordsFlattened() {
        return streamConsumerRecords().collect(Collectors.toList());
    }

    /**
     * @return a flat {@link Iterator} of the {@link RecordContext}s, which wrap the {@link ConsumerRecord}s in this
     * result set
     */
    @Override
    public Iterator<RecordContext<K, V>> iterator() {
        return stream().iterator();
    }

    /**
     * @param action to perform on the {@link RecordContext}s, which wrap the {@link ConsumerRecord}s in this result
     *               set
     */
    @Override
    public void forEach(Consumer<? super RecordContext<K, V>> action) {
        Iterable.super.forEach(action);
    }

    /**
     * @return a flat {@link Spliterator} of the {@link RecordContext}s, which wrap the {@link ConsumerRecord}s in this
     * result set
     */
    @Override
    public Spliterator<RecordContext<K, V>> spliterator() {
        return Iterable.super.spliterator();
    }

    /**
     * @return a {@link Map} of {@link TopicPartition} to {@link RecordContext} {@link Set}, which wrap the {@link
     * ConsumerRecord}s in this result set
     */
    public Map<TopicPartition, Set<RecordContext<K, V>>> getByTopicPartitionMap() {
        // unwraps the internal view of the record to a user view
        return this.records.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                                set -> set.getValue().stream()
                                        .map(RecordContextInternal::getRecordContext)
                                        .collect(Collectors.toSet())
                        )
                );
    }

    /**
     * @return the total count of records in this result set
     */
    public long size() {
        return stream().count();
    }

    /**
     * Get all the offsets for the records in this result set.
     * <p>
     * Note that this flattens the result, so if there are records from multiple {@link TopicPartition}s, the partition
     * they belong to will be lost. If you want that information as well, try {@link #getOffsets()}.
     *
     * @return a flat List of offsets in this result set
     * @see #getOffsets()
     */
    public List<Long> getOffsetsFlattened() {
        return streamConsumerRecords().mapToLong(ConsumerRecord::offset).boxed().collect(Collectors.toList());
    }

    /**
     * Map of partitions to offsets.
     * <p>
     * If you don't need the partition information, try {@link #getOffsetsFlattened()}.
     *
     * @return a map of {@link TopicPartition} to offsets, of the records in this result set
     * @see #getOffsetsFlattened()
     */
    public Map<TopicPartition, List<Long>> getOffsets() {
        return getByTopicPartitionMap().entrySet().stream()
                .collect(Collectors
                        .toMap(Map.Entry::getKey, e -> e.getValue().stream()
                                .map(RecordContext::offset).collect(Collectors.toList())
                        )
                );
    }

}
