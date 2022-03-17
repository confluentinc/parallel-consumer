package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.TimeUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.BrokerPollSystem;
import io.confluent.parallelconsumer.internal.DynamicLoadFactor;
import io.confluent.parallelconsumer.internal.EpochAndRecords;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;

import static java.lang.Boolean.TRUE;
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

//    private final WorkMailBoxManager<K, V> wmbm;

    @Getter
    private int numberRecordsOutForProcessing = 0;

    /**
     * Useful for testing
     */
    @Getter(PUBLIC)
    private final List<Consumer<WorkContainer<K, V>>> successfulWorkListeners = new ArrayList<>();

    public WorkManager(ParallelConsumerOptions<K, V> options, org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        this(options, consumer, new DynamicLoadFactor(), TimeUtils.getClock());
    }

    /**
     * Use a private {@link DynamicLoadFactor}, useful for testing.
     */
    public WorkManager(ParallelConsumerOptions<K, V> options, org.apache.kafka.clients.consumer.Consumer<K, V> consumer, Clock clock) {
        this(options, consumer, new DynamicLoadFactor(), clock);
    }

    public WorkManager(final ParallelConsumerOptions<K, V> newOptions, final org.apache.kafka.clients.consumer.Consumer<K, V> consumer,
                       final DynamicLoadFactor dynamicExtraLoadFactor, Clock clock) {
        this.options = newOptions;
        this.dynamicLoadFactor = dynamicExtraLoadFactor;
//        this.wmbm = new WorkMailBoxManager<>();
        this.sm = new ShardManager<>(options, this, clock);
        this.pm = new PartitionMonitor<>(consumer, sm, options, clock);
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
//        wmbm.onPartitionsRemoved(partitions);
    }

    /**
     * for testing only
     */
    public void registerWork(ConsumerRecords<K, V> records) {
        registerWork(new EpochAndRecords(records, 0));
    }

    public void registerWork(EpochAndRecords records) {
//        wmbm.registerWork(records);
        pm.maybeRegisterNewRecordAsWork(records);
    }

//    /**
//     * Moves the requested amount of work from initial queues into work queues, if available.
//     *
//     * @param requestedMaxWorkToRetrieve try to move at least this many messages into the inbound queues
//     * @return the number of extra records ingested due to request
//     */
//    private int ingestPolledRecordsIntoQueues(long requestedMaxWorkToRetrieve) {
//        log.debug("Will attempt to register the requested {} - {} available in internal mailbox",
//                requestedMaxWorkToRetrieve, wmbm.internalFlattenedMailQueueSize());
//
//        //
//        var takenWorkCount = 0;
//        boolean continueIngesting;
//        do {
//            ConsumerRecord<K, V> polledRecord = wmbm.internalFlattenedMailQueuePoll();
//            boolean recordAddedAsWork = pm.maybeRegisterNewRecordAsWork(polledRecord);
//            if (recordAddedAsWork) {
//                takenWorkCount++;
//            }
//            boolean polledQueueNotExhausted = polledRecord != null;
//            boolean ingestTargetNotSatisfied = takenWorkCount < requestedMaxWorkToRetrieve;
//            continueIngesting = ingestTargetNotSatisfied && polledQueueNotExhausted;
//        } while (continueIngesting);
//
//        log.debug("{} new records were registered.", takenWorkCount);
//
//        return takenWorkCount;
//    }

//    private int ingestPolledRecordsIntoQueues(long ) {
//        log.debug("Will attempt to register the requested {} - {} available in internal mailbox",
//                requestedMaxWorkToRetrieve, wmbm.internalFlattenedMailQueueSize());
//
//        //
//        var takenWorkCount = 0;
//        boolean continueIngesting;
//        do {
//            ConsumerRecord<K, V> polledRecord = wmbm.internalFlattenedMailQueuePoll();
//            boolean recordAddedAsWork = pm.maybeRegisterNewRecordAsWork(polledRecord);
//            if (recordAddedAsWork) {
//                takenWorkCount++;
//            }
//            boolean polledQueueNotExhausted = polledRecord != null;
//            boolean ingestTargetNotSatisfied = takenWorkCount < requestedMaxWorkToRetrieve;
//            continueIngesting = ingestTargetNotSatisfied && polledQueueNotExhausted;
//        } while (continueIngesting);
//
//        log.debug("{} new records were registered.", takenWorkCount);
//
//        return takenWorkCount;
//    }

    /**
     * Get work with no limit on quantity, useful for testing.
     */
    public List<WorkContainer<K, V>> getWorkIfAvailable() {
        return getWorkIfAvailable(Integer.MAX_VALUE);
    }

    /**
     * Depth first work retrieval.
     */
    public List<WorkContainer<K, V>> getWorkIfAvailable(final int requestedMaxWorkToRetrieve) {
        // optimise early
        if (requestedMaxWorkToRetrieve < 1) {
            return UniLists.of();
        }

//        int ingested = tryToEnsureQuantityOfWorkQueuedAvailable(requestedMaxWorkToRetrieve);

        //
        var work = sm.getWorkIfAvailable(requestedMaxWorkToRetrieve);

        //
        log.debug("Got {} of {} requested records of work. In-flight: {}, Awaiting in commit (partition) queues: {}",
                work.size(),
                requestedMaxWorkToRetrieve,
                getNumberRecordsOutForProcessing(),
                getNumberOfEntriesInPartitionQueues());
        numberRecordsOutForProcessing += work.size();

        return work;
    }

//    /**
//     * Tries to ensure there are at least this many records available in the queues
//     *
//     * @return the number of extra records ingested due to request
//     */
//    // todo rename - shunt messages from internal buffer into queues
//    // visible for testing
//    public int tryToEnsureQuantityOfWorkQueuedAvailable(final int requestedMaxWorkToRetrieve) {
//        // todo this counts all partitions as a whole - this may cause some partitions to starve. need to round robin it?
//        long available = sm.getNumberOfWorkQueuedInShardsAwaitingSelection();
//        long extraNeededFromInboxToSatisfy = requestedMaxWorkToRetrieve - available;
//        log.debug("Requested: {}, available in shards: {}, will try to process from mailbox the delta of: {}",
//                requestedMaxWorkToRetrieve, available, extraNeededFromInboxToSatisfy);
//
//        int ingested = ingestPolledRecordsIntoQueues(extraNeededFromInboxToSatisfy);
//        log.debug("Ingested an extra {} records", ingested);
//
//        long ingestionOffBy = extraNeededFromInboxToSatisfy - ingested;
//
//        return ingested;
//    }

    public void onSuccessResult(WorkContainer<K, V> wc) {
        log.trace("Work success ({}), removing from processing shard queue", wc);

        wc.endFlight();

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

    public void onFailureResult(WorkContainer<K, V> wc) {
        // error occurred, put it back in the queue if it can be retried
        wc.endFlight();
        pm.onFailure(wc);
        sm.onFailure(wc);
        numberRecordsOutForProcessing--;
    }

    public long getNumberOfEntriesInPartitionQueues() {
        return pm.getNumberOfEntriesInPartitionQueues();
    }

//    public Integer getAmountOfWorkQueuedWaitingIngestion() {
//        return wmbm.getAmountOfWorkQueuedWaitingIngestion();
//    }

    public Map<TopicPartition, OffsetAndMetadata> collectCommitDataForDirtyPartitions() {
        return pm.collectDirtyCommitData();
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
        return getTotalWorkAwaitingIngestion() > options.getTargetAmountOfRecordsInFlight() * getLoadingFactor();
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
//        return sm.getNumberOfEntriesInPartitionQueues
        return sm.getNumberOfWorkQueuedInShardsAwaitingSelection();
//        long workQueuedInShardsCount = sm.getNumberOfWorkQueuedInShardsAwaitingSelection();
//        Integer workQueuedInMailboxCount = getAmountOfWorkQueuedWaitingIngestion();
//        return workQueuedInShardsCount + workQueuedInMailboxCount;
    }

    public long getNumberOfWorkQueuedInShardsAwaitingSelection() {
        return sm.getNumberOfWorkQueuedInShardsAwaitingSelection();
    }

//    public boolean hasWorkAwaitingIngestionToShards() {
//        return getAmountOfWorkQueuedWaitingIngestion() > 0;
//    }

    public boolean hasWorkInCommitQueues() {
        return pm.hasWorkInCommitQueues();
    }

    public boolean isRecordsAwaitingProcessing() {
        return sm.getNumberOfWorkQueuedInShardsAwaitingSelection() > 0;
//        long partitionWorkRemainingCount = sm.getNumberOfWorkQueuedInShardsAwaitingSelection();
//        boolean internalQueuesNotEmpty = hasWorkAwaitingIngestionToShards();
//        return partitionWorkRemainingCount > 0 || internalQueuesNotEmpty;
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
            Optional<Boolean> userFunctionSucceeded = wc.getMaybeUserFunctionSucceeded();
            if (userFunctionSucceeded.isPresent()) {
                if (TRUE.equals(userFunctionSucceeded.get())) {
                    onSuccessResult(wc);
                } else {
                    onFailureResult(wc);
                }
            } else {
                throw new IllegalStateException("Work returned, but without a success flag - report a bug");
            }
        }
    }

    public boolean isNoRecordsOutForProcessing() {
        return getNumberRecordsOutForProcessing() == 0;
    }

    public Optional<Duration> getLowestRetryTime() {
        return sm.getLowestRetryTime();
    }

//    /**
//     * @return true if more records are needed to be sent out for processing (not enough in queues to satisfy
//     * concurrency target)
//     */
//    public boolean isStarvedForNewWork() {
//        long queued = getTotalWorkAwaitingIngestion();
//        return queued < options.getTargetAmountOfRecordsInFlight();
//    }
}
