package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.csid.utils.LoopingResumingIterator;
import io.confluent.csid.utils.WallClock;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.DynamicLoadFactor;
import io.confluent.parallelconsumer.internal.RateLimiter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.csid.utils.KafkaUtils.toTP;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static lombok.AccessLevel.PACKAGE;
import static lombok.AccessLevel.PUBLIC;

/**
 * Sharded, prioritised, offset managed, order controlled, delayed work queue.
 * <p>
 * Low Water Mark - the highest offset (continuously successful) with all it's previous messages succeeded (the offset
 * one commits to broker)
 * <p>
 * High Water Mark - the highest offset which has succeeded (previous may be incomplete)
 * <p>
 * Highest seen offset - the highest ever seen offset
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class WorkManager<K, V> implements ConsumerRebalanceListener {

    @Getter
    private final ParallelConsumerOptions options;

    // todo rename PSM, PartitionStateManager
    @Getter(PUBLIC)
    final PartitionMonitor<K, V> pm;

    // todo make private
    @Getter(PUBLIC)
    private final ShardManager<K, V> sm;

    /**
     * The multiple of {@link ParallelConsumerOptions#getMaxConcurrency()} that should be pre-loaded awaiting
     * processing.
     * <p>
     * We use it here as well to make sure we have a matching number of messages in queues available.
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
    @Getter(PUBLIC)
    private final List<Consumer<WorkContainer<K, V>>> successfulWorkListeners = new ArrayList<>();

    @Setter(PACKAGE)
    private WallClock clock = new WallClock();

    org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    /**
     * Get's set to true whenever work is returned completed, so that we know when a commit needs to be made.
     * <p>
     * In normal operation, this probably makes very little difference, as typical commit frequency is 1 second, so low
     * chances no work has completed in the last second.
     */
    private final AtomicBoolean workStateIsDirtyNeedsCommitting = new AtomicBoolean(false);

    // too aggressive for some situations? make configurable?
    private final Duration thresholdForTimeSpentInQueueWarning = Duration.ofSeconds(10);

    private final RateLimiter slowWarningRateLimit = new RateLimiter(5);

    /**
     * Use a private {@link DynamicLoadFactor}, useful for testing.
     */
    public WorkManager(ParallelConsumerOptions<K, V> options, org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        this(options, consumer, new DynamicLoadFactor());
    }

    public WorkManager(final ParallelConsumerOptions<K, V> newOptions, final org.apache.kafka.clients.consumer.Consumer<K, V> consumer, final DynamicLoadFactor dynamicExtraLoadFactor) {
        this.options = newOptions;
        this.consumer = consumer;
        this.dynamicLoadFactor = dynamicExtraLoadFactor;
        this.wmbm = new WorkMailBoxManager<>();
        this.sm = new ShardManager<K, V>(options);
        this.pm = new PartitionMonitor<>(consumer, sm);
    }

    /**
     * Load offset map for assigned partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        pm.onPartitionsAssigned(partitions);
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
        pm.onPartitionsRevoked(partitions);
        onPartitionsRemoved(partitions);
    }

    /**
     * Clear offset map for lost partitions
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        pm.onPartitionsLost(partitions);
        onPartitionsRemoved(partitions);
    }

    void onPartitionsRemoved(final Collection<TopicPartition> partitions) {
        wmbm.onPartitionsRemoved(partitions);
    }

    public void registerWork(ConsumerRecords<K, V> records) {
        wmbm.registerWork(records);
    }

    /**
     * @param requestedMaxWorkToRetrieve try to move at least this many messages into the inbound queues
     */
    private void ingestPolledRecordsIntoQueues(final int requestedMaxWorkToRetrieve) {
        log.debug("Will attempt to register the requested {} - {} available in internal mailbox",
                requestedMaxWorkToRetrieve, wmbm.internalFlattenedMailQueueSize());

        //
        var taken = 0;
        @SuppressWarnings("BooleanVariableAlwaysNegated")
        boolean stillProcessing;
        do {
            ConsumerRecord<K, V> poll = wmbm.internalFlattenedMailQueuePoll();
            boolean takenAsWork = maybeRegisterNewRecordAsWork(poll);
            if (takenAsWork) {
                taken++;
            }
            stillProcessing = taken < requestedMaxWorkToRetrieve && poll != null;
        } while (stillProcessing);

        log.debug("{} new records were registered.", taken);
    }

    /**
     * Takes a record as work and puts it into internal queues, unless it's been previously as completed as per loaded
     * records.
     *
     * @return true if the record was taken, false if it was skipped (previously successful)
     */
    // todo rename to maybeTakeRecord
    // todo move to PM, if returns true, sm.addWorkContainer
    private boolean maybeRegisterNewRecordAsWork(final ConsumerRecord<K, V> rec) {
        if (rec == null) return false;

        if (!pm.isPartitionAssigned(rec)) {
            log.debug("Record in buffer for a partition no longer assigned. Dropping. TP: {} rec: {}", toTP(rec), rec);
            return false;
        }

        if (pm.isRecordPreviouslyProcessed(rec)) {
            log.trace("Record previously processed, skipping. offset: {}", rec.offset());
            return false;
        } else {
            TopicPartition tp = toTP(rec);

            int currentPartitionEpoch = pm.getEpoch(rec, tp);
            var wc = new WorkContainer<>(currentPartitionEpoch, rec);

            sm.addWorkContainer(wc);

            pm.addWorkContainer(wc);

            return true;
        }
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
    // todo refactor
    public List<WorkContainer<K, V>> maybeGetWork(int requestedMaxWorkToRetrieve) {
        int workToGetDelta = requestedMaxWorkToRetrieve;

        // optimise early
        if (workToGetDelta < 1) {
            return UniLists.of();
        }

        tryToEnsureAvailableCapacity(requestedMaxWorkToRetrieve);

        //
        List<WorkContainer<K, V>> work = new ArrayList<>();

        //
        LoopingResumingIterator<Object, NavigableMap<Long, WorkContainer<K, V>>> it =
                sm.getIterator(iterationResumePoint);

        var staleWorkToRemove = new ArrayList<WorkContainer<K, V>>();

        var slowWorkCount = 0;
        var slowWorkTopics = new HashSet<String>();

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
                        // this state should never happen, as work should get removed from shards upon partition revocation
                        log.debug("Work is in queue with stale epoch. Will remove now. Was it not removed properly on revoke? Or are we in a race state? {}", workContainer);
                        staleWorkToRemove.add(workContainer);
                        continue; // skip
                    }
                }

                // TODO refactor this and the rest of the partition state monitoring code out
                // check we have capacity in offset storage to process more messages
                TopicPartition topicPartition = workContainer.getTopicPartition();
                boolean notAllowedMoreRecords = pm.isBlocked(topicPartition);
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
                        slowWorkTopics.add(workContainer.getCr().topic());
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
                            "waiting longer than {}s for following topics {}.",
                    finalSlowWorkCount, toSeconds(thresholdForTimeSpentInQueueWarning), slowWorkTopics));
        }

        // remove found stale work outside of loop
        for (final WorkContainer<K, V> kvWorkContainer : staleWorkToRemove) {
            sm.removeWorkFromShard(kvWorkContainer);
        }

        log.debug("Got {} records of work. In-flight: {}, Awaiting in commit queues: {}", work.size(), getNumberRecordsOutForProcessing(), getNumberOfEntriesInPartitionQueues());
        numberRecordsOutForProcessing += work.size();

        return work;
    }

    /**
     * Tries to ensure there are at least this many records available in the queues
     */
    // todo rename - shunt messages from internal buffer into queues
    private void tryToEnsureAvailableCapacity(final int requestedMaxWorkToRetrieve) {
        // todo this counts all partitions as a whole - this may cause some partitions to starve. need to round robin it?
        int available = sm.getWorkQueuedInShardsCount();
        int extraNeededFromInboxToSatisfy = requestedMaxWorkToRetrieve - available;
        log.debug("Requested: {}, available in shards: {}, will try to process from mailbox the delta of: {}",
                requestedMaxWorkToRetrieve, available, extraNeededFromInboxToSatisfy);

        ingestPolledRecordsIntoQueues(extraNeededFromInboxToSatisfy);
    }

    // todo move most to ShardManager
    public void onSuccess(WorkContainer<K, V> wc) {
        log.trace("Processing success...");
        workStateIsDirtyNeedsCommitting.set(true);
        ConsumerRecord<K, V> cr = wc.getCr();
        log.trace("Work success ({}), removing from processing shard queue", wc);
        wc.succeed();
        Object key = sm.computeShardKey(cr);
        // remove from processing queues
        var shard = sm.getShard(key);
        if (shard == null)
            throw new NullPointerException(msg("Shard is missing for key {}", key));
        long offset = cr.offset();
        shard.remove(offset);
        // If using KEY ordering, where the shard key is a message key, garbage collect old shard keys (i.e. KEY ordering we may never see a message for this key again)
        boolean keyOrdering = options.getOrdering().equals(KEY);
        if (keyOrdering && shard.isEmpty()) {
            log.trace("Removing empty shard (key: {})", key);
            sm.removeShard(key);
        }

        // notify listeners
        successfulWorkListeners.forEach((c) -> c.accept(wc));

        numberRecordsOutForProcessing--;
    }

    /**
     * @see PartitionMonitor#onOffsetCommitSuccess(Map)
     */
    // todo remove?
    public void onOffsetCommitSuccess(Map<TopicPartition, OffsetAndMetadata> offsetsToSend) {
        pm.onOffsetCommitSuccess(offsetsToSend);
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
    // todo move to ShardManager
    private void putBack(WorkContainer<K, V> wc) {
        log.debug("Work FAILED, returning to shard");
        ConsumerRecord<K, V> cr = wc.getCr();
        Object key = sm.computeShardKey(cr);
        var shard = sm.getShard(key);
        long offset = wc.getCr().offset();
        shard.put(offset, wc);
        numberRecordsOutForProcessing--;
    }

    public long getNumberOfEntriesInPartitionQueues() {
        return pm.getNumberOfEntriesInPartitionQueues();
    }

    public Integer getWorkQueuedInMailboxCount() {
        return wmbm.getWorkQueuedInMailboxCount();
    }

    // todo rename
    public Map<TopicPartition, OffsetAndMetadata> findCompletedEligibleOffsetsAndRemove() {
        return findCompletedEligibleOffsetsAndRemove(true);
    }

    public boolean hasCommittableOffsets() {
        return isDirty();
    }

    /**
     * TODO: This entire loop could be possibly redundant, if we instead track low water mark, and incomplete offsets as
     * work is submitted and returned.
     * <p>
     * todo: refactor into smaller methods?
     * <p>
     * todo docs
     * <p>
     * Finds eligible offset positions to commit in each assigned partition
     */
    // todo move to PartitionMonitor / PartitionState
    // todo rename
    // todo remove completely as state of offsets should be tracked live, no need to scan for them
    <R> Map<TopicPartition, OffsetAndMetadata> findCompletedEligibleOffsetsAndRemove(boolean remove) {

        //
        if (!isDirty()) {
            // nothing to commit
            return UniMaps.of();
        }

        //
        Map<TopicPartition, OffsetAndMetadata> offsetsToSend = new HashMap<>();
        int count = 0;
        int removed = 0;
        log.trace("Scanning for in order in-flight work that has completed...");

        //
        Set<Map.Entry<TopicPartition, PartitionState<K, V>>> set = pm.getPartitionStates().entrySet();
        for (final Map.Entry<TopicPartition, PartitionState<K, V>> partitionStateEntry : set) {
            var partitionState = partitionStateEntry.getValue();
            Map<Long, WorkContainer<K, V>> partitionQueue = partitionState.getCommitQueues();
            TopicPartition topicPartitionKey = partitionStateEntry.getKey();
            log.trace("Starting scan of partition: {}", topicPartitionKey);

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
                            topicPartitionKey,
                            partitionQueue.size());
                    incompleteOffsets.add(offset);
                }
            }

            pm.addEncodedOffsets(offsetsToSend, topicPartitionKey, incompleteOffsets);

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
     * Have our partitions been revoked?
     *
     * @return true if epoch doesn't match, false if ok
     */
    public boolean checkEpochIsStale(final WorkContainer<K, V> workContainer) {
        return pm.checkEpochIsStale(workContainer);
    }

    public boolean shouldThrottle() {
        return isSufficientlyLoaded();
    }

    /**
     * @return true if there's enough messages downloaded from the broker already to satisfy the pipeline, false if more
     * should be downloaded (or pipelined in the Consumer)
     */
    public boolean isSufficientlyLoaded() {
        return getWorkQueuedInMailboxCount() > options.getMaxConcurrency() * getLoadingFactor();
    }

    private int getLoadingFactor() {
        return dynamicLoadFactor.getCurrentFactor();
    }

    public boolean workIsWaitingToBeProcessed() {
        return sm.workIsWaitingToBeProcessed();
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

    /**
     * @return Work count in mailbox plus work added to the processing shards
     */
    public int getTotalWorkWaitingProcessing() {
        int workQueuedInShardsCount = sm.getWorkQueuedInShardsCount();
        Integer workQueuedInMailboxCount = getWorkQueuedInMailboxCount();
        return workQueuedInShardsCount + workQueuedInMailboxCount;
    }

    public boolean hasWorkInMailboxes() {
        return getWorkQueuedInMailboxCount() > 0;
    }

    public boolean hasWorkInCommitQueues() {
        return pm.hasWorkInCommitQueues();
    }

    public boolean isRecordsAwaitingProcessing() {
        int partitionWorkRemainingCount = sm.getWorkQueuedInShardsCount();
        boolean internalQueuesNotEmpty = hasWorkInMailboxes();
        return partitionWorkRemainingCount > 0 || internalQueuesNotEmpty;
    }

    public boolean isRecordsAwaitingToBeCommitted() {
        // todo could be improved - shouldn't need to count all entries if we simply want to know if there's > 0
        var partitionWorkRemainingCount = getNumberOfEntriesInPartitionQueues();
        return partitionWorkRemainingCount > 0;
    }

    public void handleFutureResult(WorkContainer<K, V> wc) {
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
