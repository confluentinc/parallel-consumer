package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.LoopingResumingIterator;
import io.confluent.csid.utils.WallClock;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.csid.utils.KafkaUtils.toTP;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.OffsetMapCodecManager.DefaultMaxMetadataSize;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static lombok.AccessLevel.PACKAGE;

/**
 * Sharded, prioritised, offset managed, order controlled, delayed work queue.
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class WorkManager<K, V> implements ConsumerRebalanceListener {

    /**
     * Best efforts attempt to prevent usage of offset payload beyond X% - as encoding size test is currently only done
     * per batch, we need to leave some buffer for the required space to overrun before hitting the hard limit where we
     * have to drop the offset payload entirely.
     */
    @Setter
    public static double USED_PAYLOAD_THRESHOLD_MULTIPLIER = 0.75;

    @Getter
    private final ParallelConsumerOptions options;

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
    private final Map<Object, NavigableMap<Long, WorkContainer<K, V>>> processingShards = new HashMap<>();

    /**
     * Map of partitions to Map of offsets to WorkUnits
     * <p>
     * Need to record globally consumed records, to ensure correct offset order committal. Cannot rely on incrementally
     * advancing offsets, as this isn't a guarantee of kafka's.
     * <p>
     * Concurrent because either the broker poller thread or the control thread may be requesting offset to commit
     * ({@link #findCompletedEligibleOffsetsAndRemove})
     *
     * @see #findCompletedEligibleOffsetsAndRemove
     */
    private final Map<TopicPartition, NavigableMap<Long, WorkContainer<K, V>>> partitionCommitQueues = new ConcurrentHashMap<>();

    /**
     * The multiple of {@link ParallelConsumerOptions#getMaxConcurrency()} that should be pre-loaded awaiting
     * processing. We use it here as well to make sure we have a matching number of messages in queues avaialbe.
     */
    private final DynamicLoadFactor dynamicLoadFactor;

    private final WorkMailBoxManager<K, V> wmbm;

    /**
     * Iteration resume point, to ensure fairness (prevent shard starvation) when we can't process messages from every
     * shard.
     */
    private Optional<Object> iterationResumePoint = Optional.empty();

    @Getter
    private int numberRecordsOutForProcessing = 0;

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
     * Offsets, which have been seen, beyond the highest committable offset, which haven't been totally completed
     */
    Map<TopicPartition, Set<Long>> partitionIncompleteOffsets = new HashMap<>();

    // visible for testing
    /**
     * The highest seen offset for a partition
     */
    Map<TopicPartition, Long> partitionOffsetHighWaterMarks = new HashMap<>();

    // visible for testing
    long MISSING_HIGH_WATER_MARK = -1L;

    /**
     * Get's set to true whenever work is returned completed, so that we know when a commit needs to be made.
     * <p>
     * In normal operation, this probably makes very little difference, as typical commit frequency is 1 second, so low
     * chances no work has completed in the last second.
     */
    private AtomicBoolean workStateIsDirtyNeedsCommitting = new AtomicBoolean(false);

    /**
     * If true, more messages are allowed to process for this partition.
     * <p>
     * If false, we have calculated that we can't record any more offsets for this partition, as our best performing
     * encoder requires nearly as much space is available for this partitions allocation of the maximum offset metadata
     * size.
     * <p>
     * Default (missing elements) is true - more messages can be processed.
     *
     * @see OffsetMapCodecManager#DefaultMaxMetadataSize
     */
    Map<TopicPartition, Boolean> partitionMoreRecordsAllowedToProcess = new HashMap<>();

    /**
     * Record the generations of partition asignment, for fencing off invalid work
     */
    private Map<TopicPartition, Integer> partitionsAssignmentEpochs = new HashMap<>();

    // too aggressive for some situations? make configurable?
    private final Duration thresholdForTimeSpentInQueueWarning = Duration.ofSeconds(10);

    private final RateLimiter slowWarningRateLimit = new RateLimiter(5);

    /**
     * Use a private {@link DynamicLoadFactor}, useful for testing.
     */
    public WorkManager(ParallelConsumerOptions options, org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        this(options, consumer, new DynamicLoadFactor());
    }

    public WorkManager(final ParallelConsumerOptions newOptions, final org.apache.kafka.clients.consumer.Consumer<K, V> consumer, final DynamicLoadFactor dynamicExtraLoadFactor) {
        this.options = newOptions;
        this.consumer = consumer;
        this.dynamicLoadFactor = dynamicExtraLoadFactor;
        this.wmbm = new WorkMailBoxManager<K, V>();
    }

    /**
     * Load offset map for assigned partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.debug("Partitions assigned: {}", partitions);
        incrementPartitionAssignmentEpoch(partitions);

        // init messages allowed state
        for (final TopicPartition partition : partitions) {
            partitionMoreRecordsAllowedToProcess.putIfAbsent(partition, true);
        }

        try {
            Set<TopicPartition> partitionsSet = UniSets.copyOf(partitions);
            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this, this.consumer); // todo remove throw away instance creation
            om.loadOffsetMapForPartition(partitionsSet);
        } catch (Exception e) {
            log.error("Error in onPartitionsAssigned", e);
            throw e;
        }
    }

    /**
     * Clear offset map for revoked partitions
     * <p>
     * {@link ParallelEoSStreamProcessor#onPartitionsRevoked} handles committing off offsets upon revoke
     *
     * @see ParallelEoSStreamProcessor#onPartitionsRevoked
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Partitions revoked: {}", partitions);

        try {
            onPartitionsRemoved(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsRevoked", e);
            throw e;
        }
    }

    private void onPartitionsRemoved(final Collection<TopicPartition> partitions) {
        incrementPartitionAssignmentEpoch(partitions);
        resetOffsetMapAndRemoveWork(partitions);
    }

    /**
     * Clear offset map for lost partitions
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        try {
            log.info("Lost partitions: {}", partitions);
            onPartitionsRemoved(partitions);
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
        wmbm.onPartitionsRemoved(partitions);
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
            removeWorkFromShard(work);
        }
    }

    private void removeWorkFromShard(final WorkContainer<K, V> work) {
        Object shardKey = computeShardKey(work.getCr());
        log.debug("Removing expired work {} for shard key: {}", work, shardKey);
        this.processingShards.remove(shardKey);
    }

    public void registerWork(List<ConsumerRecords<K, V>> records) {
        for (var record : records) {
            registerWork(record);
        }
    }

    public void registerWork(ConsumerRecords<K, V> records) {
        wmbm.registerWork(records);
    }

    private void processInbox(final int requestedMaxWorkToRetrieve) {
        wmbm.processInbox();

        int gap = requestedMaxWorkToRetrieve;
        int taken = 0;

        log.debug("Will attempt to register the requested {} - {} available in internal mailbox",
                requestedMaxWorkToRetrieve, wmbm.internalFlattenedMailQueueSize());

        //
        while (taken < gap && !wmbm.internalFlattenedMailQueueIsEmpty()) {
            ConsumerRecord<K, V> poll = wmbm.internalFlattenedMailQueuePoll();
            boolean takenAsWork = processInbox(poll);
            if (takenAsWork) {
                taken++;
            }
        }

        log.debug("{} new records were registered.", taken);

    }

    /**
     * @return true if the record was taken, false if it was skipped (previously successful)
     */
    private boolean processInbox(final ConsumerRecord<K, V> rec) {
        if (isRecordPreviouslyProcessed(rec)) {
            log.trace("Record previously processed, skipping. offset: {}", rec.offset());
            return false;
        } else {
            Object shardKey = computeShardKey(rec);
            long offset = rec.offset();
            TopicPartition tp = toTP(rec);

            Integer currentPartitionEpoch = partitionsAssignmentEpochs.get(tp);
            if (currentPartitionEpoch == null) {
                throw new InternalRuntimeError(msg("Received message for a partition which is not assigned: {}", rec));
            }
            var wc = new WorkContainer<>(currentPartitionEpoch, rec);

            raisePartitionHighWaterMark(offset, tp);

            processingShards.computeIfAbsent(shardKey, (ignore) -> new TreeMap<>()).put(offset, wc);

            partitionCommitQueues.computeIfAbsent(tp, (ignore) -> new ConcurrentSkipListMap<>()).put(offset, wc);

            return true;
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
        Set<Long> incompleteOffsets = this.partitionIncompleteOffsets.getOrDefault(tp, new TreeSet<>());
        boolean previouslyProcessed;
        if (incompleteOffsets.contains(offset)) {
            // record previously saved as having not been processed
            previouslyProcessed = false;
        } else {
            Long offsetHighWaterMark = partitionOffsetHighWaterMarks.getOrDefault(tp, MISSING_HIGH_WATER_MARK);
            if (offset <= offsetHighWaterMark) {
                // within the range of tracked offsets, so must have been previously completed
                previouslyProcessed = true;
            } else {
                // we haven't recorded this far up, so must not have been processed yet
                previouslyProcessed = false;
            }
        }
        log.trace("Record {} previously seen? {}", rec.offset(), previouslyProcessed);
        return previouslyProcessed;
    }

    private Object computeShardKey(ConsumerRecord<K, V> rec) {
        return switch (options.getOrdering()) {
            case KEY -> rec.key();
            case PARTITION, UNORDERED -> new TopicPartition(rec.topic(), rec.partition());
        };
    }

    /**
     * Get work with no limit on quantity, useful for testing.
     */
    public <R> List<WorkContainer<K, V>> maybeGetWork() {
        return maybeGetWork(Integer.MAX_VALUE);
    }

    /**
     * Depth first work retrieval.
     */
    public List<WorkContainer<K, V>> maybeGetWork(int requestedMaxWorkToRetrieve) {
        int workToGetDelta = requestedMaxWorkToRetrieve;

        // optimise early
        if (workToGetDelta < 1) {
            return UniLists.of();
        }

        // todo this counts all partitions as a whole - this may cause some partitions to starve. need to round robin it?
        int available = getWorkQueuedInShardsCount();
        int extraNeededFromInboxToSatisfy = requestedMaxWorkToRetrieve - available;
        log.debug("Requested: {}, available in shards: {}, will try to process from mailbox the delta of: {}",
                requestedMaxWorkToRetrieve, available, extraNeededFromInboxToSatisfy);
        processInbox(extraNeededFromInboxToSatisfy);

        //
        List<WorkContainer<K, V>> work = new ArrayList<>();

        //
        var it = new LoopingResumingIterator<>(iterationResumePoint, processingShards);

        var staleWorkToRemove = new ArrayList<WorkContainer<K, V>>();

        var slowWorkCount = 0;

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

                var workContainer = queueEntry.getValue();

                {
                    if (checkEpochIsStale(workContainer)) {
                        // this state should never happen
                        log.debug("Work is in queue with stale epoch. Will remove now. Was it not removed properly on revoke? Or are we in a race state? {}", workContainer);
                        staleWorkToRemove.add(workContainer);
                        continue; // skip
                    }
                }

                // TODO refactor this and the rest of the partition state monitoring code out
                // check we have capacity in offset storage to process more messages
                TopicPartition topicPartition = workContainer.getTopicPartition();
                boolean notAllowedMoreRecords = !partitionMoreRecordsAllowedToProcess.getOrDefault(topicPartition, true);
                // If the record has been previously attempted, it is already represented in the current offset encoding,
                // and may in fact be the message holding up the partition so must be retried, in which case we don't want to skip it
                boolean recordNeverAttempted = !workContainer.hasPreviouslyFailed();
                if (notAllowedMoreRecords && recordNeverAttempted && workContainer.isNotInFlight()) {
                    log.debug("Not allowed more records ({}) for the partition ({}) as set from previous encode run, that this " +
                                    "record ({}) belongs to due to offset encoding back pressure, has never been attemtped before ({}), " +
                                    "not in flight ({}), continuing on to next container in shard.",
                            notAllowedMoreRecords, topicPartition, workContainer.offset(), recordNeverAttempted, workContainer.isNotInFlight());
                    continue;
                }

                // check if work can be taken
                boolean hasNotSucceededAlready = !workContainer.isUserFunctionSucceeded();
                boolean delayHasPassed = workContainer.hasDelayPassed(clock);
                if (delayHasPassed && workContainer.isNotInFlight() && hasNotSucceededAlready) {
                    log.trace("Taking {} as work", workContainer);
                    workContainer.queueingForExecution();
                    shardWork.add(workContainer);
                } else {
                    Duration timeInFlight = workContainer.getTimeInFlight();
                    String msg = "Can't take as work: Work ({}). Must all be true: Delay passed= {}. Is not in flight= {}. Has not succeeded already= {}. Time spent in execution queue: {}.";
                    if (toSeconds(timeInFlight) > toSeconds(thresholdForTimeSpentInQueueWarning)) {
                        slowWorkCount++;
                        log.trace("Work has spent over " + thresholdForTimeSpentInQueueWarning + " in queue! "
                                + msg, workContainer, delayHasPassed, workContainer.isNotInFlight(), hasNotSucceededAlready, timeInFlight);
                    } else {
                        log.trace(msg, workContainer, delayHasPassed, workContainer.isNotInFlight(), hasNotSucceededAlready, timeInFlight);
                    }
                }

                ProcessingOrder ordering = options.getOrdering();
                if (ordering == UNORDERED) {
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

        if (slowWorkCount > 0) {
            final int finalSlowWorkCount = slowWorkCount;
            slowWarningRateLimit.performIfNotLimited(() -> log.warn("Warning: {} records in the queue have been " +
                            "waiting longer than {}.",
                    finalSlowWorkCount, toSeconds(thresholdForTimeSpentInQueueWarning)));

        }

        // remove found stale work outside of loop
        for (final WorkContainer<K, V> kvWorkContainer : staleWorkToRemove) {
            removeWorkFromShard(kvWorkContainer);
        }

        log.debug("Got {} records of work. In-flight: {}, Awaiting in commit queues: {}", work.size(), getNumberRecordsOutForProcessing(), getNumberOfEntriesInPartitionQueues());
        numberRecordsOutForProcessing += work.size();

        return work;
    }

    public void onSuccess(WorkContainer<K, V> wc) {
        log.trace("Processing success...");
        workStateIsDirtyNeedsCommitting.set(true);
        ConsumerRecord<K, V> cr = wc.getCr();
        log.trace("Work success ({}), removing from processing shard queue", wc);
        wc.succeed();
        Object key = computeShardKey(cr);
        // remove from processing queues
        NavigableMap<Long, WorkContainer<K, V>> shard = processingShards.get(key);
        long offset = cr.offset();
        shard.remove(offset);
        // If using KEY ordering, where the shard key is a message key, garbage collect old shard keys (i.e. KEY ordering we may never see a message for this key again)
        boolean keyOrdering = options.getOrdering().equals(KEY);
        if (keyOrdering && shard.isEmpty()) {
            log.trace("Removing empty shard (key: {})", key);
            processingShards.remove(key);
        }
        successfulWorkListeners.forEach((c) -> c.accept(wc)); // notify listeners
        numberRecordsOutForProcessing--;
    }

    public void onFailure(WorkContainer<K, V> wc) {
        // error occurred, put it back in the queue if it can be retried
        // if not explicitly retriable, put it back in with an try counter so it can be later given up on
        wc.fail(clock);
        putBack(wc);
    }

    /**
     * Idempotent - work may have not been removed, either way it's put back
     */
    private void putBack(WorkContainer<K, V> wc) {
        log.debug("Work FAILED, returning to shard");
        ConsumerRecord<K, V> cr = wc.getCr();
        Object key = computeShardKey(cr);
        var shard = processingShards.get(key);
        long offset = wc.getCr().offset();
        shard.put(offset, wc);
        numberRecordsOutForProcessing--;
    }

    public int getNumberOfEntriesInPartitionQueues() {
        int count = 0;
        for (var e : this.partitionCommitQueues.entrySet()) {
            count += e.getValue().size();
        }
        return count;
    }

    /**
     * @return Work count in mailbox plus work added to the processing shards
     */
    public int getTotalWorkWaitingProcessing() {
        int workQueuedInShardsCount = getWorkQueuedInShardsCount();
        Integer workQueuedInMailboxCount = getWorkQueuedInMailboxCount();
        return workQueuedInShardsCount + workQueuedInMailboxCount;
    }

    Integer getWorkQueuedInMailboxCount() {
        return wmbm.getWorkQueuedInMailboxCount();
    }

    /**
     * @return Work ready in the processing shards, awaiting selection as work to do
     */
    public int getWorkQueuedInShardsCount() {
        int count = 0;
        for (var e : this.processingShards.entrySet()) {
            count += e.getValue().size();
        }
        return count;
    }

    boolean isRecordsAwaitingProcessing() {
        int partitionWorkRemainingCount = getWorkQueuedInShardsCount();
        boolean internalQueuesNotEmpty = hasWorkInMailboxes();
        return partitionWorkRemainingCount > 0 || internalQueuesNotEmpty;
    }

    boolean isRecordsAwaitingToBeCommitted() {
        // todo could be improved - shouldn't need to count all entries if we simply want to know if there's > 0
        int partitionWorkRemainingCount = getNumberOfEntriesInPartitionQueues();
        return partitionWorkRemainingCount > 0;
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

    boolean hasCommittableOffsets() {
        return isDirty();
    }

    /**
     * TODO: This entire loop could be possibly redundant, if we instead track low water mark, and incomplete offsets as
     * work is submitted and returned.
     * <p>
     * todo: refactor into smaller methods?
     */
    <R> Map<TopicPartition, OffsetAndMetadata> findCompletedEligibleOffsetsAndRemove(boolean remove) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToSend = new HashMap<>();
        int count = 0;
        int removed = 0;
        log.trace("Scanning for in order in-flight work that has completed...");
        for (final var partitionQueueEntry : partitionCommitQueues.entrySet()) {
            TopicPartition topicPartitionKey = partitionQueueEntry.getKey();
            log.trace("Starting scan of partition: {}", topicPartitionKey);
            var partitionQueue = partitionQueueEntry.getValue();
            count += partitionQueue.size();
            var workToRemove = new LinkedList<WorkContainer<K, V>>();
            var incompleteOffsets = new LinkedHashSet<Long>();
            long lowWaterMark = -1;
            // can't commit this offset or beyond, as this is the latest offset that is incomplete
            // i.e. only commit offsets that come before the current one, and stop looking for more
            boolean iteratedBeyondLowWaterMarkBeingLowestCommittableOffset = false;
            for (final var offsetAndItsWorkContainer : partitionQueue.entrySet()) {
                // ordered iteration via offset keys thanks to the tree-map
                WorkContainer<K, V> container = offsetAndItsWorkContainer.getValue();
                long offset = container.getCr().offset();
                boolean complete = container.isUserFunctionComplete();
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
                        // todo lookup the low water mark and include here
                        log.trace("Offset {} is complete and succeeded, but we've iterated past the lowest committable offset ({}). Will mark as complete in the offset map.",
                                container.getCr().offset(), lowWaterMark);
                        // no-op - offset map is only for not succeeded or completed offsets
//                        // mark as complete complete so remove from work
//                        workToRemove.add(container);
                    } else {
                        log.trace("Offset {} is complete, but failed processing. Will track in offset map as not complete. Can't do normal offset commit past this point.", container.getCr().offset());
                        iteratedBeyondLowWaterMarkBeingLowestCommittableOffset = true;
                        incompleteOffsets.add(offset);
                    }
                } else {
                    lowWaterMark = container.offset();
                    iteratedBeyondLowWaterMarkBeingLowestCommittableOffset = true;
                    log.trace("Offset ({}) is incomplete, holding up the queue ({}) of size {}.",
                            container.getCr().offset(),
                            partitionQueueEntry.getKey(),
                            partitionQueueEntry.getValue().size());
                    incompleteOffsets.add(offset);
                }
            }

            addEncodedOffsets(offsetsToSend, topicPartitionKey, incompleteOffsets);

            if (remove) {
                removed += workToRemove.size();
                for (var workContainer : workToRemove) {
                    var offset = workContainer.getCr().offset();
                    partitionQueue.remove(offset);
                }
            }
        }

        log.debug("Scan finished, {} were in flight, {} completed offsets removed, coalesced to {} offset(s) ({}) to be committed",
                count, removed, offsetsToSend.size(), offsetsToSend);
        return offsetsToSend;
    }

    /**
     * Get final offset data, build the the offset map, and replace it in our map of offset data to send
     *
     * @param offsetsToSend
     * @param topicPartitionKey
     * @param incompleteOffsets
     * @return
     */
    private void addEncodedOffsets(Map<TopicPartition, OffsetAndMetadata> offsetsToSend,
                                   TopicPartition topicPartitionKey,
                                   LinkedHashSet<Long> incompleteOffsets) {
        // TODO potential optimisation: store/compare the current incomplete offsets to the last committed ones, to know if this step is needed or not (new progress has been made) - isdirty?
        boolean offsetEncodingNeeded = !incompleteOffsets.isEmpty();
        if (offsetEncodingNeeded) {
            long offsetOfNextExpectedMessage;
            OffsetAndMetadata finalOffsetOnly = offsetsToSend.get(topicPartitionKey);
            if (finalOffsetOnly == null) {
                // no new low water mark to commit, so use the last one again
                offsetOfNextExpectedMessage = incompleteOffsets.iterator().next(); // first element
            } else {
                offsetOfNextExpectedMessage = finalOffsetOnly.offset();
            }

            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this, this.consumer);
            try {
                String offsetMapPayload = om.makeOffsetMetadataPayload(offsetOfNextExpectedMessage, topicPartitionKey, incompleteOffsets);
                int metaPayloadLength = offsetMapPayload.length();
                boolean moreMessagesAllowed;
                OffsetAndMetadata offsetWithExtraMap;
                // todo move
                double pressureThresholdValue = DefaultMaxMetadataSize * USED_PAYLOAD_THRESHOLD_MULTIPLIER;

                if (metaPayloadLength > DefaultMaxMetadataSize) {
                    // exceeded maximum API allowed, strip the payload
                    moreMessagesAllowed = false;
                    offsetWithExtraMap = new OffsetAndMetadata(offsetOfNextExpectedMessage); // strip payload
                    log.warn("Offset map data too large (size: {}) to fit in metadata payload hard limit of {} - cannot include in commit. " +
                                    "Warning: messages might be replayed on rebalance. " +
                                    "See kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize = {} and issue #47.",
                            metaPayloadLength, DefaultMaxMetadataSize, DefaultMaxMetadataSize);
                } else if (metaPayloadLength > pressureThresholdValue) { // and thus metaPayloadLength <= DefaultMaxMetadataSize
                    // try to turn on back pressure before max size is reached
                    moreMessagesAllowed = false;
                    offsetWithExtraMap = new OffsetAndMetadata(offsetOfNextExpectedMessage, offsetMapPayload);
                    log.warn("Payload size {} higher than threshold {}, but still lower than max {}. Will write payload, but will " +
                                    "not allow further messages, in order to allow the offset data to shrink (via succeeding messages).",
                            metaPayloadLength, pressureThresholdValue, DefaultMaxMetadataSize);
                } else { // and thus (metaPayloadLength <= pressureThresholdValue)
                    moreMessagesAllowed = true;
                    offsetWithExtraMap = new OffsetAndMetadata(offsetOfNextExpectedMessage, offsetMapPayload);
                    log.debug("Payload size {} within threshold {}", metaPayloadLength, pressureThresholdValue);
                }

                partitionMoreRecordsAllowedToProcess.put(topicPartitionKey, moreMessagesAllowed);
                offsetsToSend.put(topicPartitionKey, offsetWithExtraMap);
            } catch (EncodingNotSupportedException e) {
                partitionMoreRecordsAllowedToProcess.put(topicPartitionKey, false);
                log.warn("No encodings could be used to encode the offset map, skipping. Warning: messages might be replayed on rebalance.", e);
            }
        } else {
            partitionMoreRecordsAllowedToProcess.put(topicPartitionKey, true);
        }
    }

    /**
     * Truncate our tracked offsets as a commit was successful, so the low water mark rises, and we dont' need to track
     * as much anymore.
     * <p>
     * When commits are made to broker, we can throw away all the individually tracked offsets before the committed
     * offset.
     */
    public void onOffsetCommitSuccess(Map<TopicPartition, OffsetAndMetadata> offsetsToSend) {
        // partitionOffsetHighWaterMarks this will get overwritten in due course
        offsetsToSend.forEach((tp, meta) -> {
            Set<Long> offsets = partitionIncompleteOffsets.get(tp);
            boolean trackedOffsetsForThisPartitionExist = offsets != null && !offsets.isEmpty();
            if (trackedOffsetsForThisPartitionExist) {
                long newLowWaterMark = meta.offset();
                offsets.removeIf(offset -> offset < newLowWaterMark);
            }
        });
    }

    /**
     * Have our partitions been revoked?
     *
     * @return true if epoch doesn't match, false if ok
     */
    boolean checkEpochIsStale(final WorkContainer<K, V> workContainer) {
        TopicPartition topicPartitionKey = workContainer.getTopicPartition();

        Integer currentPartitionEpoch = partitionsAssignmentEpochs.get(topicPartitionKey);
        int workEpoch = workContainer.getEpoch();
        if (currentPartitionEpoch != workEpoch) {
            log.debug("Epoch mismatch {} vs {} for record {} - were partitions lost? Skipping message - it's already assigned to a different consumer (possibly me).",
                    workEpoch, currentPartitionEpoch, workContainer);
            return true;
        }
        return false;
    }

    private void incrementPartitionAssignmentEpoch(final Collection<TopicPartition> partitions) {
        for (final TopicPartition partition : partitions) {
            int epoch = partitionsAssignmentEpochs.getOrDefault(partition, -1);
            epoch++;
            partitionsAssignmentEpochs.put(partition, epoch);
        }
    }

    public boolean shouldThrottle() {
        return isSufficientlyLoaded();
    }

    /**
     * @return true if there's enough messages downloaded from the broker already to satisfy the pipeline, false if more
     *         should be downloaded (or pipelined in the Consumer)
     */
    boolean isSufficientlyLoaded() {
        return getWorkQueuedInMailboxCount() > options.getMaxConcurrency() * getLoadingFactor();
    }

    private int getLoadingFactor() {
        return dynamicLoadFactor.getCurrentFactor();
    }

    public boolean workIsWaitingToBeProcessed() {
        Collection<NavigableMap<Long, WorkContainer<K, V>>> values = processingShards.values();
        for (NavigableMap<Long, WorkContainer<K, V>> value : values) {
            if (!value.isEmpty())
                return true;
        }
        return false;
    }

    public boolean hasWorkInFlight() {
        return getNumberRecordsOutForProcessing() != 0;
    }

    public boolean isClean() {
        return !isDirty();
    }

    private boolean isDirty() {
        return this.workStateIsDirtyNeedsCommitting.get();
    }

    public boolean hasWorkInMailboxes() {
        return getWorkQueuedInMailboxCount() > 0;
    }

    public boolean hasWorkInCommitQueues() {
        for (var e : this.partitionCommitQueues.entrySet()) {
            if (!e.getValue().isEmpty())
                return true;
        }
        return false;
    }


    protected void handleFutureResult(WorkContainer<K, V> wc) {
        // todo need to make sure epoch's match, as partition may have been re-assigned after being revoked see PR #46
        // partition may be revoked by a different thread (broker poller) - need to synchronise?
        // https://github.com/confluentinc/parallel-consumer/pull/46 (partition epochs)
        if (checkEpochIsStale(wc)) {
            // no op, partition has been revoked
            log.debug("Work result received, but from an old generation. Dropping work from revoked partition {}", wc);
            // todo mark work as to be skipped, instead of just returning null - related to retry system https://github.com/confluentinc/parallel-consumer/issues/48
            return;
        }

        if (wc.getUserFunctionSucceeded().get()) {
            onSuccess(wc);
        } else {
            onFailure(wc);
        }
    }

}
