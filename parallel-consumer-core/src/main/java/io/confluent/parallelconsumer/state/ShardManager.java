package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.csid.utils.LoopingResumingIterator;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.BrokerPollSystem;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.metrics.PCMetrics;
import io.confluent.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.Gauge;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
 *
 * @author Antony Stubbs
 */
// metrics: number of queues, average queue length
@Slf4j
public class ShardManager<K, V> {

    private final PCModule<K, V> module;


    @Getter
    private final ParallelConsumerOptions<?, ?> options;

    private final WorkManager<K, V> wm;

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
    // performance: could disable/remove if using partition order - but probably not worth the added complexity in the code to handle an extra special case
    @Getter(AccessLevel.PRIVATE)
    @Setter(AccessLevel.PACKAGE)
    private Map<ShardKey, ProcessingShard<K, V>> processingShards = new ConcurrentHashMap<>();

    /**
     * TreeSet is a Set, so must ensure that we are consistent with equalTo in our comparator - so include the full id -
     * {@link TopicPartition} and offset after comparing the retry due time.
     * <p>
     * I.e. two instances of WC are not equal, just because their retry due time its.
     * <p>
     * Also - our primary comparison - {@link WorkContainer#getRetryDueAt()} must return a consistant value, regardless
     * of WHEN it's queried - so must not use shortcuts like {@link Instant#now()}
     */
    @Getter(AccessLevel.PACKAGE) // visible for testing
    private final Comparator<WorkContainer<?, ?>> retryQueueWorkContainerComparator = Comparator
            .comparing((WorkContainer<?, ?> workContainer) -> workContainer.getRetryDueAt())
            .thenComparing(workContainer -> {
                // TopicPartition does not implement comparable
                TopicPartition tp = workContainer.getTopicPartition();
                return tp.topic() + tp.partition();
            })
            .thenComparing(WorkContainer::offset);

    /**
     * Read optimised view of {@link WorkContainer}s that need retrying.
     */
    @Getter(AccessLevel.PACKAGE) // visible for testing
    private final NavigableSet<WorkContainer<?, ?>> retryQueue = new TreeSet<>(retryQueueWorkContainerComparator);

    /**
     * Iteration resume point, to ensure fairness (prevent shard starvation) when we can't process messages from every
     * shard.
     */
    private Optional<ShardKey> iterationResumePoint = Optional.empty();

    private Gauge shardsSizeGauge;
    private Gauge numberOfShardsGauge;

    private final PCMetrics pcMetrics;

    public ShardManager(final PCModule<K, V> module, final WorkManager<K, V> wm) {
        this.module = module;
        this.wm = wm;
        this.options = module.options();
        this.pcMetrics = module.pcMetrics();
        initMetrics();
    }

    /**
     * The shard belonging to the given key
     *
     * @return may return empty if the shard has since been removed
     */
    Optional<ProcessingShard<K, V>> getShard(ShardKey key) {
        return Optional.ofNullable(processingShards.get(key));
    }

    ShardKey computeShardKey(WorkContainer<?, ?> wc) {
        return ShardKey.of(wc, options.getOrdering());
    }

    ShardKey computeShardKey(ConsumerRecord<?, ?> wc) {
        return ShardKey.of(wc, options.getOrdering());
    }

    /**
     * @return Work ready in the processing shards, awaiting selection as work to do
     */
    public long getNumberOfWorkQueuedInShardsAwaitingSelection() {
        // all available container count - (still pending for running retry containers count)
        // => all_available_count - (retryCnt - all_expired_retry_cnt)

        return processingShards.values().stream()
                .mapToLong(ProcessingShard::getCountOfWorkAwaitingSelection)
                .sum() - retryQueue.size() + getNumberOfFailedWorkReadyToBeRetried();
    }

    public boolean workIsWaitingToBeProcessed() {
        return getNumberOfWorkQueuedInShardsAwaitingSelection() > 0L;
    }

    /**
     * Remove only the work shards which are referenced from work from revoked partitions
     *
     * @param recordsFromRemovedPartition collection of work to scan to get keys of shards to remove
     */
    void removeAnyShardEntriesReferencedFrom(Collection<Optional<ConsumerRecord<K, V>>> recordsFromRemovedPartition) {
        List<ConsumerRecord<K, V>> polledRecordsFromPartition = recordsFromRemovedPartition.stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        for (ConsumerRecord<K, V> consumerRecord : polledRecordsFromPartition) {
            removeWorkFromShardFor(consumerRecord);
        }
    }

    /**
     * Removes any tracked work for this record, and removes the shard if it is empty
     */
    private void removeWorkFromShardFor(ConsumerRecord<K, V> consumerRecord) {
        ShardKey shardKey = computeShardKey(consumerRecord);

        if (processingShards.containsKey(shardKey)) {
            // remove the work
            ProcessingShard<K, V> shard = processingShards.get(shardKey);
            WorkContainer<K, V> removedWC = shard.remove(consumerRecord.offset());

            // remove if in retry queue
            // check null to avoid race condition
            if (Objects.nonNull(removedWC)) {
                this.retryQueue.remove(removedWC);
            }

            // remove the shard if empty
            removeShardIfEmpty(shardKey);
        } else {
            log.trace("Shard referenced by WC: {} with shard key: {} already removed", consumerRecord, shardKey);
        }

    }

    public void addWorkContainer(long epochOfInboundRecords, ConsumerRecord<K, V> aRecord) {
        var wc = new WorkContainer<>(epochOfInboundRecords, aRecord, module);
        ShardKey shardKey = computeShardKey(wc);

        // don't need to synchronise on /adding/ elements, as the iterator would just stop early
        var shard = processingShards.computeIfAbsent(shardKey,
                ignore -> new ProcessingShard<>(shardKey, options, wm.getPm()));
        shard.addWorkContainer(wc);
    }

    void removeShardIfEmpty(ShardKey key) {
        Optional<ProcessingShard<K, V>> shardOpt = getShard(key);

        // If using KEY ordering, where the shard key is a message key, garbage collect old shard keys (i.e. KEY ordering we may never see a message for this key again)
        // If not, no point to remove the shard, as it will be reused for the next message from the same partition
        boolean keyOrdering = options.getOrdering().equals(KEY);
        if (keyOrdering && shardOpt.isPresent() && shardOpt.get().isEmpty()) {
            log.trace("Removing empty shard (key: {})", key);
            this.processingShards.remove(key);
        }
    }

    public void onSuccess(WorkContainer<?, ?> wc) {
        // remove from the retry queue if it's contained
        this.retryQueue.remove(wc);

        // remove from processing queues
        var key = computeShardKey(wc);
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

        var key = computeShardKey(wc);
        var shardOptional = getShard(key);

        if (shardOptional.isPresent()) {
            shardOptional.get().onFailure();
        }

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
        LoopingResumingIterator<ShardKey, ProcessingShard<K, V>> shardQueueIterator =
                new LoopingResumingIterator<>(iterationResumePoint, this.processingShards);

        //
        List<WorkContainer<K, V>> workFromAllShards = new ArrayList<>();

        // loop over shards, and get work from each
        Optional<Map.Entry<ShardKey, ProcessingShard<K, V>>> next = shardQueueIterator.next();
        while (workFromAllShards.size() < requestedMaxWorkToRetrieve && next.isPresent()) {
            var shardEntry = next;
            ProcessingShard<K, V> shard = shardEntry.get().getValue();

            //
            int remainingToGet = requestedMaxWorkToRetrieve - workFromAllShards.size();
            var work = shard.getWorkIfAvailable(remainingToGet);
            workFromAllShards.addAll(work);

            // next
            next = shardQueueIterator.next();
        }

        // log
        if (workFromAllShards.size() >= requestedMaxWorkToRetrieve) {
            log.debug("Work taken is now over max (iteration resume point is {})", iterationResumePoint);
        }

        //
        updateResumePoint(next);

        return workFromAllShards;
    }

    // remove stale containers from both processingShards and retryQueue
    public boolean removeStaleContainers() {
        return processingShards.values().stream()
                .map(ProcessingShard::removeStaleWorkContainersFromShard)
                .flatMap(Collection::stream)
                .map(retryQueue::remove)
                .findAny().isPresent();
    }

    private void updateResumePoint(Optional<Map.Entry<ShardKey, ProcessingShard<K, V>>> lastShard) {
        // if empty, iteration was exhausted and no resume point is needed
        iterationResumePoint = lastShard.map(Map.Entry::getKey);
        if (iterationResumePoint.isPresent()) {
            log.debug("Work taken is now over max, stopping (saving iteration resume point {})", iterationResumePoint);
        }
    }

    private void initMetrics() {
        shardsSizeGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.SHARDS_SIZE,
                this, shardManager -> shardManager.processingShards.values().stream()
                        .mapToInt(processingShard -> processingShard.getEntries().size()).sum());
        numberOfShardsGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.NUMBER_OF_SHARDS,
                this, shardManager -> shardManager.processingShards.keySet().size());
    }

    // get expired items count from retryQueue
    private long getNumberOfFailedWorkReadyToBeRetried() {
        long count = 0;
        for (WorkContainer<?, ?> workContainer : retryQueue) {
            // when poller check, considering it is ready to be retried. there are two scenarios:
            // 1. the container is yet to be selected, therefore it is not inflight and should be counted in
            // 2. the container has been selected and it is inflight but we already slashed them from availableWorkContainerCnt, so should be counted in
            if (workContainer.isDelayPassed()) {
                count++;
            } else {
                // early stop since retryQueue is sorted by retryDueAt
                break;
            }
        }
        return count;
    }
}
