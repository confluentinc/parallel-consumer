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

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static java.util.Optional.empty;
import static java.util.Optional.of;

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
    // todo introduce an actual Shard object to store the map, will make things clearer
    private final Map<Object, NavigableMap<Long, WorkContainer<K, V>>> processingShards = new ConcurrentHashMap<>();

    private final NavigableSet<WorkContainer<?, ?>> retryQueue = new TreeSet<>(Comparator.comparing(WorkContainer::getDelayUntilRetryDue));

    /**
     * The shard belonging to the given key
     *
     * @return may return empty if the shard has since been removed
     */
    // todo don't expose inner data structures - wrap instead
    Optional<NavigableMap<Long, WorkContainer<K, V>>> getShard(Object key) {
        return Optional.ofNullable(processingShards.get(key));
    }

    LoopingResumingIterator<Object, NavigableMap<Long, WorkContainer<K, V>>> getIterator(final Optional<Object> iterationResumePoint) {
        return new LoopingResumingIterator<>(iterationResumePoint, this.processingShards);
    }

    Object computeShardKey(ConsumerRecord<?, ?> rec) {
        return switch (options.getOrdering()) {
            case KEY -> rec.key();
            case PARTITION, UNORDERED -> new TopicPartition(rec.topic(), rec.partition());
        };
    }

    Object computeShardKey(WorkContainer<?, ?> wc) {
        return computeShardKey(wc.getCr());
    }

    public WorkContainer<K, V> getWorkContainerForRecord(ConsumerRecord<K, V> rec) {
        Object key = computeShardKey(rec);
        var shard = this.processingShards.get(key);
        long offset = rec.offset();
        WorkContainer<K, V> wc = shard.get(offset);
        return wc;
    }

    /**
     * @return Work ready in the processing shards, awaiting selection as work to do
     */
    public long getNumberOfWorkQueuedInShardsAwaitingSelection() {
        long count = this.processingShards.values().parallelStream()
                .flatMap(x -> x.values().stream())
                // todo missing pm.isBlocked(topicPartition) ?
                .filter(WorkContainer::isAvailableToTakeAsWork)
                .count();
        return count;
    }

    public boolean workIsWaitingToBeProcessed() {
        Collection<NavigableMap<Long, WorkContainer<K, V>>> allShards = processingShards.values();
        for (NavigableMap<Long, WorkContainer<K, V>> oneShard : allShards) {
            if (oneShard.values().parallelStream()
                    .anyMatch(WorkContainer::isAvailableToTakeAsWork))
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

        if (processingShards.containsKey(shardKey)) {
            NavigableMap<Long, WorkContainer<K, V>> shard = processingShards.get(shardKey);
            shard.remove(work.offset());
            if (shard.isEmpty()) {
                log.debug("Removing shard referenced by WC: {} for shard key: {}", work, shardKey);
                removeShardIfEmpty(shardKey);
            }
        } else {
            log.trace("Shard referenced by WC: {} with shard key: {} already removed", work, shardKey);
        }

        //
        this.retryQueue.remove(work);
    }

    public void addWorkContainer(final WorkContainer<K, V> wc) {
        Object shardKey = computeShardKey(wc.getCr());
        var shard = processingShards.computeIfAbsent(shardKey,
                // uses a ConcurrentSkipListMap instead of a TreeMap as under high pressure there appears to be some
                // concurrency errors (missing WorkContainers)
                (ignore) -> new ConcurrentSkipListMap<>());
        long key = wc.offset();
        if (shard.containsKey(key)) {
            log.debug("Entry for {} already exists in shard queue", wc);
        } else {
            shard.put(key, wc);
        }
    }

    void removeShardIfEmpty(final Object key) {
        Optional<NavigableMap<Long, WorkContainer<K, V>>> shardOpt = getShard(key);

        // If using KEY ordering, where the shard key is a message key, garbage collect old shard keys (i.e. KEY ordering we may never see a message for this key again)
        boolean keyOrdering = options.getOrdering().equals(KEY);
        if (keyOrdering && shardOpt.isPresent() && shardOpt.get().isEmpty()) {
            log.trace("Removing empty shard (key: {})", key);
            this.processingShards.remove(key);
        }
    }

    public void onSuccess(WorkContainer<?, ?> wc) {
        //
        this.retryQueue.remove(wc);

        // remove from processing queues
        Object key = computeShardKey(wc);
        var shardOptional = getShard(key);
        if (shardOptional.isPresent()) {
            var shard = shardOptional.get();
            // remove work from shard's queue
            shard.remove(wc.offset());

            removeShardIfEmpty(key);
        } else {
            log.trace("Dropping successful result for revoked partition {}. Record in question was: {}", key, wc.getCr());
        }
    }

    /**
     * Idempotent - work may have not been removed, either way it's put back
     */
    public void onFailure(WorkContainer<?, ?> wc) {
        log.debug("Work FAILED");
        this.retryQueue.add(wc);
    }

    /**
     * @return none if there are no messages to retry
     */
    public Optional<Duration> getLowestRetryTime() {
        // find the first in the queue that isn't in flight
        // could potentially remove from queue when in flight but that's messy and performance gain would be trivial
        for (WorkContainer<?, ?> workContainer : this.retryQueue) {
            if (workContainer.isNotInFlight())
                return of(workContainer.getDelayUntilRetryDue());
        }
        return empty();
    }

}
