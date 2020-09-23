package io.confluent.csid.asyncconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.asyncconsumer.AsyncConsumerOptions.ProcessingOrder;
import io.confluent.csid.utils.LoopingResumingIterator;
import io.confluent.csid.utils.WallClock;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;

import static io.confluent.csid.utils.KafkaUtils.toTP;
import static java.lang.Math.min;
import static lombok.AccessLevel.PACKAGE;

/**
 * Sharded, prioritised, offset managed, order controlled, delayed work queue.
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class WorkManager<K, V> implements ConsumerRebalanceListener {

    @Getter
    private final AsyncConsumerOptions options;

    // todo performance: disable/remove if using partition order
    /**
     * Map of Object keys to Map of offset to WorkUnits
     * <p>
     * Object is either the K key type, or it is a {@link TopicPartition}
     * <p>
     * Used to collate together a queue of work units for each unique key consumed
     *
     * @see K
     * @see #maybeGetWork()
     */
    final private Map<Object, NavigableMap<Long, WorkContainer<K, V>>> processingShards = new ConcurrentHashMap<>();

    /**
     * Map of partitions to Map of offsets to WorkUnits
     * <p>
     * Need to record globally consumed records, to ensure correct offset order committal. Cannot rely on incrementally
     * advancing offsets, as this isn't a guarantee of kafka's.
     *
     * @see #findCompletedEligibleOffsetsAndRemove
     */
    final private Map<TopicPartition, NavigableMap<Long, WorkContainer<K, V>>> partitionCommitQueues = new ConcurrentHashMap<>();

    /**
     * Iteration resume point, to ensure fairness (prevent shard starvation) when we can't process messages from every
     * shard.
     */
    private Optional<Object> iterationResumePoint = Optional.empty();

    private int inFlightCount = 0;

    /**
     * The multiple of {@link AsyncConsumerOptions#getMaxConcurrency()} that should be pre-loaded awaiting processing.
     * Consumer already pipelines, so we shouldn't need to pipeline ourselves too much.
     */
    private final int loadingFactor = 2;

    /**
     * Useful for testing
     */
    @Getter(PACKAGE)
    private final List<Consumer<WorkContainer<K, V>>> successfulWorkListeners = new ArrayList<>();

    @Setter(PACKAGE)
    private WallClock clock = new WallClock();

    org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    // visible for testing
    /**
     * Offsets, beyond the highest committable offset, which haven't been totally completed
     */
    Map<TopicPartition, TreeSet<Long>> partitionIncompleteOffsets = new HashMap<>();

    // visible for testing
    /**
     * The highest seen offset for a partition
     */
    Map<TopicPartition, Long> partitionOffsetHighWaterMarks = new HashMap<TopicPartition, Long>();

    // visible for testing
    long MISSING_HIGH_WATER_MARK = -1L;

    public WorkManager(AsyncConsumerOptions options, org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        this.options = options;
        this.consumer = consumer;
    }

    /**
     * Load offset map for assigned partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        try {
            log.debug("onPartitionsAssigned: {}", partitions);
            Set<TopicPartition> partitionsSet = UniSets.copyOf(partitions);
            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this, this.consumer);
            om.loadOffsetMapForPartition(partitionsSet);
        } catch (Exception e) {
            log.error("Error in onPartitionsAssigned", e);
            throw e;
        }
    }

    /**
     * Clear offset map for revoked partitions
     * <p>
     * {@link AsyncConsumer#onPartitionsRevoked} handles committing off offsets upon revoke
     *
     * @see AsyncConsumer#onPartitionsRevoked
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        try {
            log.debug("Partitions revoked: {}", partitions);
            resetOffsetMapAndRemoveWork(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsRevoked", e);
            throw e;
        }
    }

    /**
     * Clear offset map for lost partitions
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        try {
            log.warn("Partitions have been lost");
            log.debug("Lost partitions: {}", partitions);
            resetOffsetMapAndRemoveWork(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsLost", e);
            throw e;
        }
    }

    private void resetOffsetMapAndRemoveWork(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            partitionIncompleteOffsets.remove(partition);
            partitionOffsetHighWaterMarks.remove(partition);
            NavigableMap<Long, WorkContainer<K, V>> oldWorkPartitionQueue = partitionCommitQueues.remove(partition);
            if (oldWorkPartitionQueue != null) {
                removeShardsFoundIn(oldWorkPartitionQueue);
            } else {
                log.trace("Removing empty commit queue");
            }
        }
    }

    /**
     * Remove only the work shards which are referenced from revoked partitions
     *
     * @param oldWorkPartitionQueue partition set to scan for unique keys to be removed from our shard queue
     */
    private void removeShardsFoundIn(NavigableMap<Long, WorkContainer<K, V>> oldWorkPartitionQueue) {
        // this all scanning loop could be avoided if we also store a map of unique keys found referenced when a
        // partition is assigned, but that could worst case grow forever
        for (WorkContainer<K, V> work : oldWorkPartitionQueue.values()) {
            K key = work.getCr().key();
            this.processingShards.remove(key);
        }
    }

    public void registerWork(List<ConsumerRecords<K, V>> records) {
        for (var record : records) {
            registerWork(record);
        }
    }

    /**
     * Work must be registered in offset order
     *
     * @see #raisePartitionHighWaterMark
     */
    public void registerWork(ConsumerRecords<K, V> records) {
        log.debug("Registering {} records of work", records.count());
        for (ConsumerRecord<K, V> rec : records) {
            if (!isRecordPreviouslyProcessed(rec)) {
                Object shardKey = computeShardKey(rec);
                long offset = rec.offset();
                var wc = new WorkContainer<K, V>(rec);

                TopicPartition tp = toTP(rec);
                raisePartitionHighWaterMark(offset, tp);

                processingShards.computeIfAbsent(shardKey, (ignore) -> new ConcurrentSkipListMap<>())
                        .put(offset, wc);

                partitionCommitQueues.computeIfAbsent(tp, (ignore) -> new ConcurrentSkipListMap<>())
                        .put(offset, wc);
            } else {
                log.trace("Record previously processed, skipping. offset: {}", rec.offset());
            }
        }
    }

    void raisePartitionHighWaterMark(long highWater, TopicPartition tp) {
        // rise the high water mark
        Long oldHighWaterMark = partitionOffsetHighWaterMarks.getOrDefault(tp, MISSING_HIGH_WATER_MARK);
        if (highWater >= oldHighWaterMark || highWater == MISSING_HIGH_WATER_MARK) {
            partitionOffsetHighWaterMarks.put(tp, highWater);
        }
    }

    private boolean isRecordPreviouslyProcessed(ConsumerRecord<K, V> rec) {
        long offset = rec.offset();
        TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
        TreeSet<Long> incompleteOffsets = this.partitionIncompleteOffsets.getOrDefault(tp, new TreeSet<>());
        if (incompleteOffsets.contains(offset)) {
            // record previously saved as having not been processed
            return false;
        } else {
            Long offsetHighWaterMarks = partitionOffsetHighWaterMarks.getOrDefault(tp, MISSING_HIGH_WATER_MARK);
            if (offset < offsetHighWaterMarks) {
                // within the range of tracked offsets, so must have been previously completed
                return true;
            } else {
                // we haven't recorded this far up, so must not have been processed yet
                return false;
            }
        }
    }

    private Object computeShardKey(ConsumerRecord<K, V> rec) {
        return switch (options.getOrdering()) {
            case KEY -> rec.key();
            case PARTITION, UNORDERED -> new TopicPartition(rec.topic(), rec.partition());
        };
    }

    public <R> List<WorkContainer<K, V>> maybeGetWork() {
        return maybeGetWork(options.getMaxConcurrency());
    }

    /**
     * Depth first work retrieval.
     *
     * @param requestedMaxWorkToRetrieve ignored unless less than {@link AsyncConsumerOptions#getMaxConcurrency()}
     */
    public List<WorkContainer<K, V>> maybeGetWork(int requestedMaxWorkToRetrieve) {
        int minWorkToGetSetting = min(min(requestedMaxWorkToRetrieve, options.getMaxConcurrency()), options.getMaxUncommittedMessagesToHandlePerPartition());
        int workToGetDelta = minWorkToGetSetting - getInFlightCount();

        // optimise early
        if (workToGetDelta < 1) {
            return UniLists.of();
        }

        //
        List<WorkContainer<K, V>> work = new ArrayList<>();

        //
        var it = new LoopingResumingIterator<>(iterationResumePoint, processingShards);

        //
        for (var shard : it) {
            log.trace("Looking for work on shard: {}", shard.getKey());
            if (work.size() >= workToGetDelta) {
                this.iterationResumePoint = Optional.of(shard.getKey());
                log.debug("Work taken is now over max, stopping (saving iteration resume point {})", iterationResumePoint);
                break;
            }

            ArrayList<WorkContainer<K, V>> shardWork = new ArrayList<>();
            SortedMap<Long, WorkContainer<K, V>> shardQueue = shard.getValue();

            // then iterate over shardQueue queue
            Set<Map.Entry<Long, WorkContainer<K, V>>> shardQueueEntries = shardQueue.entrySet();
            for (var queueEntry : shardQueueEntries) {
                int taken = work.size() + shardWork.size();
                if (taken >= workToGetDelta) {
                    log.trace("Work taken ({}) exceeds max ({})", taken, workToGetDelta);
                    break;
                }

                var wc = queueEntry.getValue();
                boolean alreadySucceeded = !wc.isUserFunctionSucceeded();
                if (wc.hasDelayPassed(clock) && wc.isNotInFlight() && alreadySucceeded) {
                    log.trace("Taking {} as work", wc);
                    wc.takingAsWork();
                    shardWork.add(wc);
                } else {
                    log.trace("Work ({}) still delayed or is in flight, can't take...", wc);
                }

                if (options.getOrdering() == ProcessingOrder.UNORDERED) {
                    // continue - we don't care about processing order, so check the next message
                    continue;
                } else {
                    // can't take any more from this partition until this work is finished
                    // processing blocked on this partition, continue to next partition
                    log.trace("Processing by {}, so have cannot get more messages on this ({}) shard.", this.options.getOrdering(), shard.getKey());
                    break;
                }
            }
            work.addAll(shardWork);
        }

        log.debug("Returning {} records of work", work.size());
        inFlightCount += work.size();
        return work;
    }

    public void success(WorkContainer<K, V> wc) {
        ConsumerRecord<K, V> cr = wc.getCr();
        log.trace("Work success ({}), removing from queue", wc);
        wc.succeed();
        Object key = computeShardKey(cr);
        processingShards.get(key).remove(cr.offset());
        successfulWorkListeners.forEach((c) -> c.accept(wc)); // notify listeners
        inFlightCount--;
    }

    public void failed(WorkContainer<K, V> wc) {
        wc.fail(clock);
        putBack(wc);
    }

    private void putBack(WorkContainer<K, V> wc) {
        log.debug("Work FAILED, returning to queue");
        ConsumerRecord<K, V> cr = wc.getCr();
        Object key = computeShardKey(cr);
        var queue = processingShards.get(key);
        long offset = wc.getCr().offset();
        queue.put(offset, wc);
        inFlightCount--;
    }

    public int getPartitionWorkRemainingCount() {
        int count = 0;
        for (var e : this.partitionCommitQueues.entrySet()) {
            count += e.getValue().size();
        }
        return count;
    }

    public int getShardWorkRemainingCount() {
        int count = 0;
        for (var e : this.processingShards.entrySet()) {
            count += e.getValue().size();
        }
        return count;
    }

    boolean isWorkRemaining() {
        return getPartitionWorkRemainingCount() > 0;
    }

    public WorkContainer<K, V> getWorkContainerForRecord(ConsumerRecord<K, V> rec) {
        Object key = computeShardKey(rec);
        var longWorkContainerTreeMap = this.processingShards.get(key);
        long offset = rec.offset();
        WorkContainer<K, V> wc = longWorkContainerTreeMap.get(offset);
        return wc;
    }

    Map<TopicPartition, OffsetAndMetadata> findCompletedEligibleOffsetsAndRemove() {
        return findCompletedEligibleOffsetsAndRemove(true);
    }

    boolean hasComittableOffsets() {
        return findCompletedEligibleOffsetsAndRemove(false).size() != 0;
    }

    /**
     * TODO: This entire loop could be possibly redundant, if we instead track low water mark, and incomplete offsets as work is submitted and returned.
     */
    @SneakyThrows
    <R> Map<TopicPartition, OffsetAndMetadata> findCompletedEligibleOffsetsAndRemove(boolean remove) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToSend = new HashMap<>();
        int count = 0;
        int removed = 0;
        log.trace("Scanning for in order in-flight work that has completed...");
        int totalOffsetMetaSize = 0;
        for (final var partitionQueueEntry : partitionCommitQueues.entrySet()) {
            TopicPartition topicPartitionKey = partitionQueueEntry.getKey();
            log.trace("Starting scan of partition: {}", topicPartitionKey);
            var partitionQueue = partitionQueueEntry.getValue();
            count += partitionQueue.size();
            var workToRemove = new LinkedList<WorkContainer<K, V>>();
            var incompleteOffsets = new LinkedHashSet<Long>();
            // can't commit this offset or beyond, as this is the latest offset that is incomplete
            // i.e. only commit offsets that come before the current one, and stop looking for more
            boolean iteratedBeyondLowWaterMarkBeingLowestCommittableOffset = false;
            for (final var offsetAndItsWorkContainer : partitionQueue.entrySet()) {
                // ordered iteration via offset keys thanks to the tree-map
                WorkContainer<K, V> container = offsetAndItsWorkContainer.getValue();
                boolean complete = container.isUserFunctionComplete();
                long offset = container.getCr().offset();
                if (complete) {
                    if (container.getUserFunctionSucceeded().get() && !iteratedBeyondLowWaterMarkBeingLowestCommittableOffset) {
                        log.trace("Found offset candidate ({}) to add to offset commit map", container);
                        workToRemove.add(container);
                        // as in flights are processed in order, this will keep getting overwritten with the highest offset available
                        // current offset is the highest successful offset, so commit +1 - offset to be committed is defined as the offset of the next expected message to be read
                        long offsetOfNextExpectedMessageToBeCommitted = offset + 1;
                        OffsetAndMetadata offsetData = new OffsetAndMetadata(offsetOfNextExpectedMessageToBeCommitted);
                        offsetsToSend.put(topicPartitionKey, offsetData);
                    } else if (container.getUserFunctionSucceeded().get() && iteratedBeyondLowWaterMarkBeingLowestCommittableOffset) {
                        log.trace("Offset {} is complete and succeeded, but we've iterated past the lowest committable offset. Will mark as complete in the offset map.", container.getCr().offset());
                        // no-op - offset map is only for not succeeded or completed offsets
//                        // mark as complete complete so remove from work
//                        workToRemove.add(container);
                    } else {
                        log.trace("Offset {} is complete, but failed processing. Will track in offset map as not complete. Can't do normal offset commit past this point.", container.getCr().offset());
                        iteratedBeyondLowWaterMarkBeingLowestCommittableOffset = true;
                        incompleteOffsets.add(offset);
                    }
                } else {
                    iteratedBeyondLowWaterMarkBeingLowestCommittableOffset = true;
                    log.trace("Offset ({}) is incomplete, holding up the queue ({}) of size {}.",
                            container.getCr().offset(),
                            partitionQueueEntry.getKey(),
                            partitionQueueEntry.getValue().size());
                    incompleteOffsets.add(offset);
                }
            }

            // offset map building
            // Get final offset data, build the the offset map, and replace it in our map of offset data to send
            // TODO potential optimisation: store/compare the current incomplete offsets to the last committed ones, to know if this step is needed or not (new progress has been made)
            if (!incompleteOffsets.isEmpty()) {
                long offsetOfNextExpectedMessage;
                OffsetAndMetadata finalOffsetOnly = offsetsToSend.get(topicPartitionKey);
                if (finalOffsetOnly == null) {
                    // no new low water mark to commit, so use the last one again
                    offsetOfNextExpectedMessage = incompleteOffsets.iterator().next(); // first element
                } else {
                    offsetOfNextExpectedMessage = finalOffsetOnly.offset();
                }

                OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this, this.consumer);
                String offsetMapPayload = om.makeOffsetMetadataPayload(offsetOfNextExpectedMessage, topicPartitionKey, incompleteOffsets);
                totalOffsetMetaSize += offsetMapPayload.length();
                OffsetAndMetadata offsetWithExtraMap = new OffsetAndMetadata(offsetOfNextExpectedMessage, offsetMapPayload);
                offsetsToSend.put(topicPartitionKey, offsetWithExtraMap);
            }

            if (remove) {
                removed += workToRemove.size();
                for (var w : workToRemove) {
                    var offset = w.getCr().offset();
                    partitionQueue.remove(offset);
                }
            }
        }

        maybeStripOffsetPayload(offsetsToSend, totalOffsetMetaSize);

        log.debug("Scan finished, {} were in flight, {} completed offsets removed, coalesced to {} offset(s) ({}) to be committed",
                count, removed, offsetsToSend.size(), offsetsToSend);
        return offsetsToSend;
    }

    /**
     * Once all the offset maps have been calculated, check if they're too big, and if so, remove all of them.
     * <p>
     * Implication of this is that if the system has to recover from this offset, then it will have to replay all the messages that were otherwise complete.
     *
     * @see OffsetMapCodecManager#DefaultMaxMetadataSize
     */
    private void maybeStripOffsetPayload(Map<TopicPartition, OffsetAndMetadata> offsetsToSend, int totalOffsetMetaSize) {
        // TODO: Potential optimisation: if max metadata size is shared across partitions, the limit used could be relative to the number of
        //  partitions assigned to this consumer. In which case, we could derive the limit for the number of downloaded but not committed
        //  offsets, from this max over some estimate. This way we limit the possibility of hitting the hard limit imposed in the protocol, thus
        //  retaining the offset map feature, at the cost of potential performance by hitting a soft maximum in our uncommitted concurrent processing.
        if (totalOffsetMetaSize > OffsetMapCodecManager.DefaultMaxMetadataSize) {
            log.warn("Offset map data too large (size: {}) to fit in metadata payload - stripping offset map out. " +
                    "See kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize = 4096",
                    totalOffsetMetaSize);
            // strip payload
            for (var entry : offsetsToSend.entrySet()) {
                TopicPartition key = entry.getKey();
                OffsetAndMetadata v = entry.getValue();
                OffsetAndMetadata stripped = new OffsetAndMetadata(v.offset(), v.toString());
                offsetsToSend.replace(key, stripped);
            }
        }
    }

    /**
     * Truncate our tracked offsets as a commit was successful, so the low water mark rises, and we dont' need to track
     * as much anymore.
     * <p>
     * When commits are made to broker, we can throw away all the individually tracked offsets before the committed offset.
     */
    public void onOffsetCommitSuccess(Map<TopicPartition, OffsetAndMetadata> offsetsToSend) {
        // partitionOffsetHighWaterMarks this will get overwritten in due course
        offsetsToSend.forEach((tp, meta) -> {
            Set<Long> offsets = partitionIncompleteOffsets.get(tp);
            boolean trackedOffsetsForThisPartitionExist = offsets != null;
            if (trackedOffsetsForThisPartitionExist) {
                long newLowWaterMark = meta.offset();
                offsets.removeIf(offset -> offset < newLowWaterMark);
            }
        });
    }

    public boolean shouldThrottle() {
        return isOverMax();
    }

    private boolean isOverMax() {
        int remaining = getPartitionWorkRemainingCount();
        boolean loadedEnough = remaining > options.getMaxConcurrency() * loadingFactor;
        boolean overMaxInFlight = remaining > options.getMaxUncommittedMessagesToHandlePerPartition();
        boolean isOverMax = loadedEnough || overMaxInFlight;
        if (isOverMax) {
            log.debug("loadedEnough {} || overMaxInFlight {}", loadedEnough, overMaxInFlight);
        }
        return isOverMax;
    }

    public int getInFlightCount() {
        return inFlightCount;
    }

    public boolean workIsWaitingToBeCompletedSuccessfully() {
        Collection<NavigableMap<Long, WorkContainer<K, V>>> values = processingShards.values();
        for (NavigableMap<Long, WorkContainer<K, V>> value : values) {
            if (!value.isEmpty())
                return true;
        }
        return false;
    }

    public boolean hasWorkInFlight() {
        return getInFlightCount() != 0;
    }

}
