package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.LoopingResumingIterator;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.BrokerPollSystem;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;

/**
 * Shards are local queues of work to be processed.
 * <p>
 * Generally they are keyed by one of the corresponding {@link ProcessingOrder} modes - key, partition etc...
 * <p>
 * This state is shared between the {@link BrokerPollSystem} thread (write - adding and removing shards and work)  and
 * the {@link AbstractParallelEoSStreamProcessor} Controller thread (read - how many records are in the shards?), so
 * must be thread safe.
 */
@Slf4j
@RequiredArgsConstructor
public class ShardManager<K, V> {

    @Getter
    private final ParallelConsumerOptions options;

    /**
     * Map of Object keys to Map of offset to WorkUnits
     * <p>
     * Object is either the K key type, or it is a {@link TopicPartition}
     * <p>
     * Used to collate together a queue of work units for each unique key consumed
     *
     * @see K
     * @see WorkManager#maybeGetWorkIfAvailable()
     */
    // todo performance: disable/remove if using partition order
    private final Map<Object, NavigableMap<Long, WorkContainer<K, V>>> processingShards = new ConcurrentHashMap<>();

    /**
     * The shard belonging to the given key
     *
     * @return may return empty if the shard has since been removed
     */
    Optional<NavigableMap<Long, WorkContainer<K, V>>> getShard(Object key) {
        return Optional.ofNullable(processingShards.get(key));
    }

    LoopingResumingIterator<Object, NavigableMap<Long, WorkContainer<K, V>>> getIterator(final Optional<Object> iterationResumePoint) {
        return new LoopingResumingIterator<>(iterationResumePoint, this.processingShards);
    }

    Object computeShardKey(ConsumerRecord<K, V> rec) {
        return switch (options.getOrdering()) {
            case KEY -> rec.key();
            case PARTITION, UNORDERED -> new TopicPartition(rec.topic(), rec.partition());
        };
    }

    public WorkContainer<K, V> getWorkContainerForRecord(ConsumerRecord<K, V> rec) {
        Object key = computeShardKey(rec);
        var longWorkContainerTreeMap = this.processingShards.get(key);
        long offset = rec.offset();
        WorkContainer<K, V> wc = longWorkContainerTreeMap.get(offset);
        return wc;
    }

    /**
     * @return Work ready in the processing shards, awaiting selection as work to do
     */
    public int getWorkQueuedInShardsCount() {
        return this.processingShards.values().stream()
                .mapToInt(Map::size)
                .sum();
    }

    public boolean workIsWaitingToBeProcessed() {
        Collection<NavigableMap<Long, WorkContainer<K, V>>> values = processingShards.values();
        for (NavigableMap<Long, WorkContainer<K, V>> value : values) {
            if (!value.isEmpty())
                return true;
        }
        return false;
    }

    /**
     * Remove only the work shards which are referenced from work from revoked partitions
     *
     * @param workFromRemovedPartition collection of work to scan to get keys of shards to remove
     */
    void removeAnyShardsReferencedBy(NavigableMap<Long, WorkContainer<K, V>> workFromRemovedPartition) {
        for (WorkContainer<K, V> work : workFromRemovedPartition.values()) {
            removeShardFor(work);
        }
    }

    private void removeShardFor(final WorkContainer<K, V> work) {
        Object shardKey = computeShardKey(work.getCr());
        log.debug("Removing shard referenced by WC: {} for shard key: {}", work, shardKey);
        this.processingShards.remove(shardKey);
    }

    public void addWorkContainer(final WorkContainer<K, V> wc) {
        Object shardKey = computeShardKey(wc.getCr());
        processingShards.computeIfAbsent(shardKey,
                        // uses a ConcurrentSkipListMap instead of a TreeMap as under high pressure there appears to be some
                        // concurrency errors (missing WorkContainers)
                        (ignore) -> new ConcurrentSkipListMap<>())
                .put(wc.offset(), wc);
    }

    void removeShard(final Object key) {
        this.processingShards.remove(key);
    }

    public void onSuccess(ConsumerRecord<K, V> cr) {
        Object key = computeShardKey(cr);
        // remove from processing queues
        var shardOptional = getShard(key);
        if (shardOptional.isPresent()) {
            long offset = cr.offset();
            var shard = shardOptional.get();
            shard.remove(offset);
            // If using KEY ordering, where the shard key is a message key, garbage collect old shard keys (i.e. KEY ordering we may never see a message for this key again)
            boolean keyOrdering = options.getOrdering().equals(KEY);
            if (keyOrdering && shard.isEmpty()) {
                log.trace("Removing empty shard (key: {})", key);
                removeShard(key);
            }
        } else {
            log.trace("Dropping successful result for revoked partition {}. Record in question was: {}", key, cr);
        }
    }

    /**
     * Idempotent - work may have not been removed, either way it's put back
     */
    public void onFailure(WorkContainer<K, V> wc) {
        log.debug("Work FAILED, returning to shard");
        addWorkContainer(wc);
    }
}
