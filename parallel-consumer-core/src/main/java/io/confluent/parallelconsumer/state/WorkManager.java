package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.LoopingResumingIterator;
import io.confluent.csid.utils.WallClock;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.BrokerPollSystem;
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

import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;

import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.lang.Boolean.TRUE;
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
 * <p>
 * This state is shared between the {@link BrokerPollSystem} thread and the {@link AbstractParallelEoSStreamProcessor}.
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class WorkManager<K, V> implements ConsumerRebalanceListener {

    @Getter
    private final ParallelConsumerOptions options;

    // todo rename PSM, PartitionStateManager
    // todo make private
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
     * {@link AbstractParallelEoSStreamProcessor#onPartitionsRevoked} handles committing off offsets upon revoke
     *
     * @see AbstractParallelEoSStreamProcessor#onPartitionsRevoked
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
     * Moves the requested amount of work from initial queues into work queues, if available.
     *
     * @param requestedMaxWorkToRetrieve try to move at least this many messages into the inbound queues
     * @return the number of extra records ingested due to request
     */
    private int ingestPolledRecordsIntoQueues(long requestedMaxWorkToRetrieve) {
        log.debug("Will attempt to register the requested {} - {} available in internal mailbox",
                requestedMaxWorkToRetrieve, wmbm.internalFlattenedMailQueueSize());

        //
        var takenWorkCount = 0;
        boolean continueIngesting;
        do {
            ConsumerRecord<K, V> polledRecord = wmbm.internalFlattenedMailQueuePoll();
            boolean recordAddedAsWork = pm.maybeRegisterNewRecordAsWork(polledRecord);
            if (recordAddedAsWork) {
                takenWorkCount++;
            }
            boolean polledQueueNotExhausted = polledRecord != null;
            boolean ingestTargetNotSatisfied = takenWorkCount < requestedMaxWorkToRetrieve;
            continueIngesting = ingestTargetNotSatisfied && polledQueueNotExhausted;
        } while (continueIngesting);

        log.debug("{} new records were registered.", takenWorkCount);

        return takenWorkCount;
    }

    /**
     * Get work with no limit on quantity, useful for testing.
     */
    public <R> List<WorkContainer<K, V>> maybeGetWorkIfAvailable() {
        return maybeGetWorkIfAvailable(Integer.MAX_VALUE);
    }

    /**
     * Depth first work retrieval.
     */
    // todo refactor - move into it's own class perhaps
    public List<WorkContainer<K, V>> maybeGetWorkIfAvailable(int requestedMaxWorkToRetrieve) {
        int workToGetDelta = requestedMaxWorkToRetrieve;

        // optimise early
        if (workToGetDelta < 1) {
            return UniLists.of();
        }

        int ingested = tryToEnsureQuantityOfWorkQueuedAvailable(requestedMaxWorkToRetrieve);

        //
        List<WorkContainer<K, V>> workFromAllShards = new ArrayList<>();

        //
        LoopingResumingIterator<Object, NavigableMap<Long, WorkContainer<K, V>>> shardQueueIterator =
                sm.getIterator(iterationResumePoint);

        var slowWorkCount = 0;
        var slowWorkTopics = new HashSet<String>();

        //
        for (var shardQueueEntry : shardQueueIterator) {
            log.trace("Looking for work on shardQueueEntry: {}", shardQueueEntry.getKey());
            if (workFromAllShards.size() >= workToGetDelta) {
                this.iterationResumePoint = Optional.of(shardQueueEntry.getKey());
                log.debug("Work taken is now over max, stopping (saving iteration resume point {})", iterationResumePoint);
                break;
            }

            ArrayList<WorkContainer<K, V>> shardWorkToTake = new ArrayList<>();
            SortedMap<Long, WorkContainer<K, V>> shard = shardQueueEntry.getValue();

            // then iterate over shardQueueEntry queue
            Set<Map.Entry<Long, WorkContainer<K, V>>> shardEntries = shard.entrySet();
            for (var shardEntry : shardEntries) {
                int taken = workFromAllShards.size() + shardWorkToTake.size();
                if (taken >= workToGetDelta) {
                    log.trace("Work taken ({}) exceeds max ({})", taken, workToGetDelta);
                    break;
                }

                var workContainer = shardEntry.getValue();

                {
                    if (checkIfWorkIsStale(workContainer)) {
                        // this state is rare, as shards or work get removed upon partition revocation, although under busy
                        // load it might occur we don't synchronize over PartitionState here so it's a bit racey, but is
                        // handled and eventually settles
                        log.debug("Work is in queue with stale epoch or no longer assigned. Skipping. Shard it came from will/was removed during partition revocation. WC: {}", workContainer);
                        continue; // skip
                    }
                }

                // TODO refactor this and the rest of the partition state monitoring code out
                // check we have capacity in offset storage to process more messages
                TopicPartition topicPartition = workContainer.getTopicPartition();
                boolean notAllowedMoreRecords = pm.isBlocked(topicPartition);
                // If the record is below the highest succeeded offset, it is already represented in the current offset encoding,
                // and may in fact be the message holding up the partition so must be retried, in which case we don't want to skip it.
                // Generally speaking, completing more offsets below the highest succeeded (and thus the set represented in the encoded payload),
                // should usually reduce the payload size requirements
                PartitionState<K, V> partitionState = pm.getPartitionState(topicPartition);
                boolean representedInEncodedPayloadAlready = workContainer.offset() < partitionState.getOffsetHighestSucceeded();
                if (notAllowedMoreRecords && !representedInEncodedPayloadAlready && workContainer.isNotInFlight()) {
                    log.debug("Not allowed more records for the partition ({}) as set from previous encode run (blocked), that this " +
                                    "record ({}) belongs to due to offset encoding back pressure, is within the encoded payload already (offset lower than highest succeeded, " +
                                    "not in flight ({}), continuing on to next container in shardEntry.",
                            topicPartition, workContainer.offset(), workContainer.isNotInFlight());
                    continue;
                }

                // check if work can be taken
                boolean hasNotSucceededAlready = !workContainer.isUserFunctionSucceeded();
                boolean delayHasPassed = workContainer.hasDelayPassed(clock);
                if (delayHasPassed && workContainer.isNotInFlight() && hasNotSucceededAlready) {
                    log.trace("Taking {} as work", workContainer);
                    workContainer.onQueueingForExecution();
                    shardWorkToTake.add(workContainer);
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
                    // noinspection UnnecessaryContinue
                    continue; // NOSONAR: in the name of self documenting code
                } else {
                    // can't take anymore from this partition until this work is finished
                    // processing blocked on this partition, continue to next partition
                    log.trace("Processing by {}, so have cannot get more messages on this ({}) shardEntry.", this.options.getOrdering(), shardEntry.getKey());
                    break;
                }
            }
            workFromAllShards.addAll(shardWorkToTake);
        }

        if (slowWorkCount > 0) {
            final int finalSlowWorkCount = slowWorkCount;
            slowWarningRateLimit.performIfNotLimited(() -> log.warn("Warning: {} records in the queue have been " +
                            "waiting longer than {}s for following topics {}.",
                    finalSlowWorkCount, toSeconds(thresholdForTimeSpentInQueueWarning), slowWorkTopics));
        }

        log.debug("Got {} records of work. In-flight: {}, Awaiting in commit (partition) queues: {}", workFromAllShards.size(), getNumberRecordsOutForProcessing(), getNumberOfEntriesInPartitionQueues());
        numberRecordsOutForProcessing += workFromAllShards.size();

        return workFromAllShards;
    }

    /**
     * Tries to ensure there are at least this many records available in the queues
     *
     * @return the number of extra records ingested due to request
     */
    // todo rename - shunt messages from internal buffer into queues
    private int tryToEnsureQuantityOfWorkQueuedAvailable(final int requestedMaxWorkToRetrieve) {
        // todo this counts all partitions as a whole - this may cause some partitions to starve. need to round robin it?
        long available = sm.getNumberOfWorkQueuedInShardsAwaitingSelection();
        long extraNeededFromInboxToSatisfy = requestedMaxWorkToRetrieve - available;
        log.debug("Requested: {}, available in shards: {}, will try to process from mailbox the delta of: {}",
                requestedMaxWorkToRetrieve, available, extraNeededFromInboxToSatisfy);

        int ingested = ingestPolledRecordsIntoQueues(extraNeededFromInboxToSatisfy);
        log.debug("Ingested an extra {} records", ingested);

        long ingestionOffBy = extraNeededFromInboxToSatisfy - ingested;

        return ingested;
    }

    // todo move PM or SM?
    public void onSuccess(WorkContainer<K, V> wc) {
        log.trace("Work success ({}), removing from processing shard queue", wc);

        wc.succeed();

        // update as we go
        pm.onSuccess(wc);
        sm.onSuccess(wc);

        // notify listeners
        successfulWorkListeners.forEach(c -> c.accept(wc));

        numberRecordsOutForProcessing--;
    }

    /**
     * Can run from controller or poller thread, depending on which is responsible for committing
     *
     * @see PartitionMonitor#onOffsetCommitSuccess(Map)
     */
    public void onOffsetCommitSuccess(Map<TopicPartition, OffsetAndMetadata> committed) {
        pm.onOffsetCommitSuccess(committed);
    }

    public void onFailure(WorkContainer<K, V> wc) {
        // error occurred, put it back in the queue if it can be retried
        wc.fail(clock);
        sm.onFailure(wc);
        numberRecordsOutForProcessing--;
    }

    public long getNumberOfEntriesInPartitionQueues() {
        return pm.getNumberOfEntriesInPartitionQueues();
    }

    public Integer getAmountOfWorkQueuedWaitingIngestion() {
        return wmbm.getAmountOfWorkQueuedWaitingIngestion();
    }

    // todo rename
    public Map<TopicPartition, OffsetAndMetadata> findCompletedEligibleOffsetsAndRemove() {
        return pm.findCompletedEligibleOffsetsAndRemove();
    }

    /**
     * Have our partitions been revoked? Can a batch contain messages of different epochs?
     *
     * @return true if any epoch is stale, false if not
     * @see #checkIfWorkIsStale(WorkContainer)
     */
    public boolean checkIfWorkIsStale(final List<WorkContainer<K, V>> workContainers) {
        for (final WorkContainer<K, V> workContainer : workContainers) {
            if (checkIfWorkIsStale(workContainer)) return true;
        }
        return false;
    }

    /**
     * Have our partitions been revoked?
     *
     * @return true if epoch doesn't match, false if ok
     */
    public boolean checkIfWorkIsStale(final WorkContainer<K, V> workContainer) {
        return pm.checkIfWorkIsStale(workContainer);
    }

    public boolean shouldThrottle() {
        return isSufficientlyLoaded();
    }

    /**
     * @return true if there's enough messages downloaded from the broker already to satisfy the pipeline, false if more
     * should be downloaded (or pipelined in the Consumer)
     */
    public boolean isSufficientlyLoaded() {
        return getAmountOfWorkQueuedWaitingIngestion() > options.getTargetAmountOfRecordsInFlight() * getLoadingFactor();
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

    public boolean isWorkInFlightMeetingTarget() {
        return getNumberRecordsOutForProcessing() >= options.getTargetAmountOfRecordsInFlight();
    }

    /**
     * @return Work count in mailbox plus work added to the processing shards
     */
    public long getTotalWorkAwaitingIngestion() {
        long workQueuedInShardsCount = sm.getNumberOfWorkQueuedInShardsAwaitingSelection();
        Integer workQueuedInMailboxCount = getAmountOfWorkQueuedWaitingIngestion();
        return workQueuedInShardsCount + workQueuedInMailboxCount;
    }

    public boolean hasWorkAwaitingIngestionToShards() {
        return getAmountOfWorkQueuedWaitingIngestion() > 0;
    }

    public boolean hasWorkInCommitQueues() {
        return pm.hasWorkInCommitQueues();
    }

    public boolean isRecordsAwaitingProcessing() {
        long partitionWorkRemainingCount = sm.getNumberOfWorkQueuedInShardsAwaitingSelection();
        boolean internalQueuesNotEmpty = hasWorkAwaitingIngestionToShards();
        return partitionWorkRemainingCount > 0 || internalQueuesNotEmpty;
    }

    public boolean isRecordsAwaitingToBeCommitted() {
        // todo could be improved - shouldn't need to count all entries if we simply want to know if there's > 0
        var partitionWorkRemainingCount = getNumberOfEntriesInPartitionQueues();
        return partitionWorkRemainingCount > 0;
    }

    public void handleFutureResult(WorkContainer<K, V> wc) {
        if (checkIfWorkIsStale(wc)) {
            // no op, partition has been revoked
            log.debug("Work result received, but from an old generation. Dropping work from revoked partition {}", wc);
        } else {
            Optional<Boolean> userFunctionSucceeded = wc.getUserFunctionSucceeded();
            if (userFunctionSucceeded.isPresent()) {
                if (TRUE.equals(userFunctionSucceeded.get())) {
                    onSuccess(wc);
                } else {
                    onFailure(wc);
                }
            } else {
                throw new IllegalStateException("Work returned, but without a success flag - report a bug");
            }
        }
    }

    public boolean isNoRecordsOutForProcessing() {
        return getNumberRecordsOutForProcessing() == 0;
    }

    // todo replace raw ConsumerRecord with read only context object wrapper #216
    public Optional<WorkContainer<K, V>> getWorkContainerFor(ConsumerRecord<K, V> rec) {
        ShardManager<K, V> shard = getSm();
        return Optional.ofNullable(shard.getWorkContainerForRecord(rec));
    }

    public Optional<Duration> getLowestRetryTime() {
        return sm.getLowestRetryTime();
    }

    /**
     * @return true if more records are needed to be sent out for processing (not enough in queues to satisfy
     * concurrency target)
     */
    public boolean isStarvedForNewWork() {
        long queued = getTotalWorkAwaitingIngestion();
        return queued < options.getTargetAmountOfRecordsInFlight();
    }
}
