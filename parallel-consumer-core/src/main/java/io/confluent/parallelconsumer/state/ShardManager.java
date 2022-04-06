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

import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static lombok.AccessLevel.PRIVATE;

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

    private final WorkManager<K, V> wm;

    @Getter(PRIVATE)
    private final Clock clock;

    /**
     * Map of Object keys to Shard
     * <p>
     * Object Type is either the K key type, or it is a {@link TopicPartition}
     * <p>
     * Used to collate together a queue of work units for each unique key consumed
     *
     * @see ProcessingShard
     * @see K
     * @see WorkManager#getWorkIfAvailable()
     */
    // todo performance: disable/remove if using partition order
//    @Getter
    private final Map<Object, ProcessingShard<K, V>> processingShards = new ConcurrentHashMap<>();

    private final NavigableSet<WorkContainer<?, ?>> retryQueue = new TreeSet<>(Comparator.comparing(wc -> wc.getDelayUntilRetryDue()));

    /**
     * Iteration resume point, to ensure fairness (prevent shard starvation) when we can't process messages from every
     * shard.
     */
    private Optional<Object> iterationResumePoint = Optional.empty();

    /**
     * The shard belonging to the given key
     *
     * @return may return empty if the shard has since been removed
     */
    // todo don't expose inner data structures - wrap instead
    Optional<ProcessingShard<K, V>> getShard(Object key) {
        return Optional.ofNullable(processingShards.get(key));
    }

    private LoopingResumingIterator<Object, ProcessingShard<K, V>> getIterator(final Optional<Object> iterationResumePoint) {
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

    /**
     * @return Work ready in the processing shards, awaiting selection as work to do
     */
    public long getNumberOfWorkQueuedInShardsAwaitingSelection() {
        return processingShards.values().stream()
                .mapToLong(ProcessingShard::getCountOfWorkAwaitingSelection)
                .sum();
    }

    public boolean workIsWaitingToBeProcessed() {
        Collection<ProcessingShard<K, V>> allShards = processingShards.values();
        return allShards.parallelStream()
                .anyMatch(ProcessingShard::workIsWaitingToBeProcessed);
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
            ProcessingShard<K, V> shard = processingShards.get(shardKey);
            shard.remove(work.offset());
            removeShardIfEmpty(shardKey);
        } else {
            log.trace("Shard referenced by WC: {} with shard key: {} already removed", work, shardKey);
        }

        //
        this.retryQueue.remove(work);
    }

    public void addWorkContainer(final WorkContainer<K, V> wc) {
        Object shardKey = computeShardKey(wc.getCr());
        var shard = processingShards.computeIfAbsent(shardKey,
                (ignore) -> new ProcessingShard<>(shardKey, options, wm.getPm()));
        shard.addWorkContainer(wc);
    }

    void removeShardIfEmpty(final Object key) {
        Optional<ProcessingShard<K, V>> shardOpt = getShard(key);

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
            //
            shardOptional.get().onSuccess(wc);

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

    public List<WorkContainer<K, V>> getWorkIfAvailable(final int requestedMaxWorkToRetrieve) {
        LoopingResumingIterator<Object, ProcessingShard<K, V>> shardQueueIterator = getIterator(iterationResumePoint);

        //
        List<WorkContainer<K, V>> workFromAllShards = new ArrayList<>();

        //
        while (workFromAllShards.size() < requestedMaxWorkToRetrieve && shardQueueIterator.hasNext()) {
            var shardEntry = shardQueueIterator.next();
            ProcessingShard<K, V> shard = shardEntry.getValue();

            //
            int remainingToGet = requestedMaxWorkToRetrieve - workFromAllShards.size();
            var work = shard.getWorkIfAvailable(remainingToGet);
            workFromAllShards.addAll(work);
        }

        // log
        if (workFromAllShards.size() >= requestedMaxWorkToRetrieve) {
            log.debug("Work taken is now over max (iteration resume point is {})", iterationResumePoint);
        }

        //
        updateResumePoint(shardQueueIterator);

        return workFromAllShards;
    }

    private void updateResumePoint(LoopingResumingIterator<Object, ProcessingShard<K, V>> shardQueueIterator) {
        if (shardQueueIterator.hasNext()) {
            var shardEntry = shardQueueIterator.next();
            this.iterationResumePoint = Optional.of(shardEntry.getKey());
            log.debug("Work taken is now over max, stopping (saving iteration resume point {})", iterationResumePoint);
        }
    }

}
