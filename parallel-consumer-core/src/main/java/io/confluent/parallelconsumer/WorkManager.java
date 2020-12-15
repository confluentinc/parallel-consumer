package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.LoopingResumingIterator;
import io.confluent.csid.utils.Range;
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
import org.slf4j.MDC;
import org.slf4j.event.Level;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.csid.utils.KafkaUtils.toTP;
import static io.confluent.csid.utils.LogUtils.at;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static lombok.AccessLevel.PACKAGE;

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
    private final Map<Object, NavigableMap<Long, WorkContainer<K, V>>> processingShards = new ConcurrentHashMap<>();

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
    //    private final Map<TopicPartition, NavigableMap<Long, WorkContainer<K, V>>> partitionCommitQueues = new HashMap<>();

    /**
     * Continuous offset encodings
     */
    private final Map<TopicPartition, OffsetSimultaneousEncoder> partitionContinuousOffsetEncoders = new ConcurrentHashMap<>();

//    private final BackoffAnalyser backoffer;

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
     * todo docs The multiple that should be pre-loaded awaiting processing. Consumer already pipelines, so we shouldn't
     * need to pipeline ourselves too much.
     * todo docs
     * The multiple that should be pre-loaded awaiting processing. Consumer already pipelines, so we shouldn't need to
     * pipeline ourselves too much.
     * <p>
     * Note how this relates to {@link BrokerPollSystem#getLongPollTimeout()} - if longPollTimeout is high and loading
     * factor is low, there may not be enough messages queued up to satisfy demand.
     */
    private final int loadingFactor = 3;

    /**
     * Useful for testing
     */
    @Getter(PACKAGE)
    private final List<Consumer<WorkContainer<K, V>>> successfulWorkListeners = new ArrayList<>();

    @Setter(PACKAGE)
    private WallClock clock = new WallClock();

    ConsumerManager consumerMgr;

    // visible for testing
    /**
     * A subset of Offsets, beyond the highest committable offset, which haven't been totally completed.
     * <p>
     * We only need to know the full incompletes when we do the {@link #findCompletedEligibleOffsetsAndRemove} scan, so
     * find the full sent only then, and discard. Otherwise, for continuous encoding, the encoders track it them
     * selves.
     * <p>
     * We work with incompletes, instead of completes, because it's a bet that most of the time the storage space for
     * storing the incompletes in memory will be smaller.
     *
     * @see #findCompletedEligibleOffsetsAndRemove(boolean)
     * @see #encodeWorkResult(boolean, WorkContainer)
     * @see #onSuccess(WorkContainer)
     * @see #onFailure(WorkContainer)
     */
    Map<TopicPartition, Set<Long>> partitionOffsetsIncompleteMetadataPayloads = new ConcurrentHashMap<>();

    // visible for testing
    /**
     * The highest seen offset for a partition
     */
    Map<TopicPartition, Long> partitionOffsetHighestSeen = new HashMap<>();

    /**
     * Highest offset which has completed
     */
    Map<TopicPartition, Long> partitionOffsetHighestSucceeded = new ConcurrentHashMap<>();

    /**
     * If true, more messages are allowed to process for this partition.
     * <p>
     * If false, we have calculated that we can't record any more offsets for this partition, as our best performing
     * encoder requires nearly as much space is available for this partitions allocation of the maximum offset metadata
     * size.
     * <p>
     * Default (missing elements) is true - more messages can be processed.
     *
     * @see #manageOffsetEncoderSpaceRequirements()
     * @see OffsetMapCodecManager#DefaultMaxMetadataSize
     */
    Map<TopicPartition, Boolean> partitionMoreRecordsAllowedToProcess = new ConcurrentHashMap<>();

    /**
     * Highest committable offset - the end offset of the highest (from the lowest seen) continuous set of completed
     * offsets. AKA low water mark.
     */
    Map<TopicPartition, Long> partitionOffsetHighestContinuousSucceeded = new ConcurrentHashMap<>();

    // visible for testing
    long MISSING_HIGHEST_SEEN = -1L;

    /**
     * Get's set to true whenever work is returned completed, so that we know when a commit needs to be made.
     * <p>
     * In normal operation, this probably makes very little difference, as typical commit frequency is 1 second, so low
     * chances no work has completed in the last second.
     */
    private AtomicBoolean workStateIsDirtyNeedsCommitting = new AtomicBoolean(false);

    private int numberOfAssignedPartitions;

    // TODO remove
    public WorkManager(ParallelConsumerOptions options, ConsumerManager consumer) {
        this(options, consumer, new DynamicLoadFactor());
    }

    public WorkManager(final ParallelConsumerOptions newOptions, final ConsumerManager consumer, final DynamicLoadFactor dynamicExtraLoadFactor) {
        this.options = newOptions;
        this.consumerMgr = consumer;
        this.dynamicLoadFactor = dynamicExtraLoadFactor;
        this.wmbm = new WorkMailBoxManager<K, V>();

        //        backoffer = new BackoffAnalyser(options.getMaxConcurrency() * 10);
    }

    Map<TopicPartition, Integer> partitionsAssignmentEpochs = new HashMap<>();

    /**
     * Load offset map for assigned partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        incrementPartitionAssignmentEpoch(partitions);

        // init messages allowed state
        for (final TopicPartition partition : partitions) {
            partitionMoreRecordsAllowedToProcess.putIfAbsent(partition, true);
        }

        numberOfAssignedPartitions = numberOfAssignedPartitions + partitions.size();
        log.info("Assigned {} partitions - that's {} bytes per partition for encoding offset overruns",
                numberOfAssignedPartitions, OffsetMapCodecManager.DefaultMaxMetadataSize / numberOfAssignedPartitions);

        try {
            log.debug("onPartitionsAssigned: {}", partitions);
            Set<TopicPartition> partitionsSet = UniSets.copyOf(partitions);
            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<K, V>(this, this.consumerMgr);
            om.loadOffsetMapForPartition(partitionsSet);
        } catch (Exception e) {
            log.error("Error in onPartitionsAssigned", e);
            throw e;
        }
    }

    private void incrementPartitionAssignmentEpoch(final Collection<TopicPartition> partitions) {
        for (final TopicPartition partition : partitions) {
            int epoch = partitionsAssignmentEpochs.getOrDefault(partition, -1);
            epoch++;
            partitionsAssignmentEpochs.put(partition, epoch);
        }
    }

    private List<TopicPartition> partitionsToRemove = new ArrayList<>();

    /**
     * Clear offset map for revoked partitions
     * <p>
     * {@link ParallelEoSStreamProcessor#onPartitionsRevoked} handles committing off offsets upon revoke
     *
     * @see ParallelEoSStreamProcessor#onPartitionsRevoked
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        incrementPartitionAssignmentEpoch(partitions);

        numberOfAssignedPartitions = numberOfAssignedPartitions - partitions.size();

        try {
            log.debug("Partitions revoked: {}", partitions);
//            removePartitionFromRecordsAndShardWork(partitions);
            registerPartitionsToBeRemoved(partitions);
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
        incrementPartitionAssignmentEpoch(partitions);

        numberOfAssignedPartitions = numberOfAssignedPartitions - partitions.size();

        try {
            log.warn("Partitions have been lost: {}", partitions);
//            log.debug("Lost partitions: {}", partitions);
//            removePartitionFromRecordsAndShardWork(partitions);
            registerPartitionsToBeRemoved(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsLost", e);
            throw e;
        }
    }

    /**
     * Called by other threads (broker poller) to be later removed inline by control.
     */
    private void registerPartitionsToBeRemoved(Collection<TopicPartition> partitions) {
        partitionsToRemove.addAll(partitions);
    }

    private void removePartitionFromRecordsAndShardWork() {
        for (TopicPartition partition : partitionsToRemove) {
            log.debug("Removing records for partition {}", partition);
            // todo is there a safer way than removing these?
            partitionOffsetHighestSeen.remove(partition);
            partitionOffsetHighestSucceeded.remove(partition);
            partitionOffsetHighestContinuousSucceeded.remove(partition);
            partitionOffsetsIncompleteMetadataPayloads.remove(partition);
//            partitionMoreRecordsAllowedToProcess.remove(partition);

            //
            NavigableMap<Long, WorkContainer<K, V>> oldWorkPartitionCommitQueue = partitionCommitQueues.remove(partition);

            //
            if (oldWorkPartitionCommitQueue != null) {
                removeShardsFoundIn(oldWorkPartitionCommitQueue);
            } else {
                log.trace("Removed empty commit queue");
            }
        }
    }

    /**
     * Remove only the work shards which are referenced from revoked partitions
     *
     * @param oldWorkPartitionQueue partition set to scan for unique keys to be removed from our shard queue
     */
    private void removeShardsFoundIn(NavigableMap<Long, WorkContainer<K, V>> oldWorkPartitionQueue) {
        log.trace("Searching for and removing work found in shard queue");
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
     * @see WorkMailBoxManager#registerWork(ConsumerRecords)
     */
    public void registerWork(ConsumerRecords<K, V> records) {
        wmbm.registerWork(records);
    }

    private void processInbox(final int requestedMaxWorkToRetrieve) {
        wmbm.processInbox(requestedMaxWorkToRetrieve);

        if (requestedMaxWorkToRetrieve < 1) {
            // none requested
            return;
        }

        //
//        int inFlight = getNumberOfEntriesInPartitionQueues();
//        int max = getMaxToGoBeyondOffset();
//        int gap = max - inFlight;
        int gap = requestedMaxWorkToRetrieve;
        int taken = 0;

//        log.debug("Will register {} (max configured: {}) records of work ({} already registered)", gap, max, inFlight);
        Queue<ConsumerRecord<K, V>> internalFlattenedMailQueue = wmbm.getInternalFlattenedMailQueue();
        log.debug("Will attempt to register and get {} requested records, {} available", requestedMaxWorkToRetrieve, internalFlattenedMailQueue.size());

        // process individual records
        while (taken < gap && !internalFlattenedMailQueue.isEmpty()) {
            ConsumerRecord<K, V> poll = internalFlattenedMailQueue.poll();
            boolean takenAsWork = processInbox(poll);
            if (takenAsWork) {
                taken++;
            }
        }

        log.debug("{} new records were registered.", taken);

//        ArrayList<ConsumerRecords<K, V>> toRemove = new ArrayList<>();
//        for (final ConsumerRecords<K, V> records : internalBatchMailQueue) {
//            records.
//
//        }
//        boolean moreRecordsCanBeAccepted = processInbox(records);
//        if (moreRecordsCanBeAccepted)
//            toRemove.add(records);
//        internalBatchMailQueue.removeAll(toRemove);
    }

//    private int getMaxToGoBeyondOffset() {
//        return backoffer.getCurrentTotalMaxCountBeyondOffset();
//    }

//    /**
//     * @return true if the records were accepted, false if they cannot be
//     * @see #processInbox()
//     */
//    private boolean processInbox(ConsumerRecords<K, V> records) {
//        int partitionWorkRemainingCount = getWorkQueuedInShardsCount();
//        int recordsToAdd = records.count();
//        // we don't break up individual record sets (although we could, but "overhead") so need to queue up records even if it goes over by some amount
//        boolean overMax = partitionWorkRemainingCount - recordsToAdd >= getMaxToGoBeyondOffset();
//        if (overMax) {
//            log.debug("Work remaining in partition queues has surpassed max, so won't bring further messages in from the pipeline queued: {} / max: {}",
//                    partitionWorkRemainingCount, getMaxToGoBeyondOffset());
//            return false;
//        }
//
////        if (!inboundOffsetWidthWithinRange(records)) {
////            return false;
////        }
//
//        //
//        log.debug("Registering {} records of work ({} already registered)", recordsToAdd, partitionWorkRemainingCount);
//
//        for (ConsumerRecord<K, V> rec : records) {
//            processInbox(rec);
//        }
//
//        return true;
//    }

    private boolean inboundOffsetWidthWithinRange(final ConsumerRecords<K, V> records) {
        // brute force - surely very slow. surely this info can be cached?
        Map<TopicPartition, List<ConsumerRecord<K, V>>> inbound = new HashMap<>();
        for (final ConsumerRecord<K, V> record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            inbound.computeIfAbsent(tp, (ignore) -> new ArrayList<>()).add(record);
        }

        Set<Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>>> inboundPartitionQueues = inbound.entrySet();
        for (final Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>> inboundPartitionQueue : inboundPartitionQueues) {
            // get highest start offset
            long start = 0l;
            TopicPartition tp = inboundPartitionQueue.getKey();
            NavigableMap<Long, WorkContainer<K, V>> longWorkContainerNavigableMap = partitionCommitQueues.get(tp);
            if (longWorkContainerNavigableMap != null) {
                for (final Map.Entry<Long, WorkContainer<K, V>> longWorkContainerEntry : longWorkContainerNavigableMap.entrySet()) {
                    WorkContainer<K, V> value = longWorkContainerEntry.getValue();
                    boolean userFunctionSucceeded = value.isUserFunctionSucceeded();
                    if (!userFunctionSucceeded) {
                        start = value.getCr().offset();

                        // now find any record what would make the width too big. Binary search?
                        // brute force
                        List<ConsumerRecord<K, V>> inboundRecordQueue = inboundPartitionQueue.getValue();
//                        ConsumerRecord<K, V> highestOffsetInboundRecord = inboundRecordQueue.get(inboundRecordQueue.size() - 1);
//                        long newEnd = highestOffsetInboundRecord.offset();

                        for (final ConsumerRecord<K, V> inboundRecord : inboundRecordQueue) {
                            long newEnd = inboundRecord.offset();
                            long width = newEnd - start;

                            if (width >= BitsetEncoder.MAX_LENGTH_ENCODABLE) {
                                long oldWidth = partitionOffsetHighestSeen.get(tp) - start;
                                // can't be more accurate unless we break up the inbound records and count them per queue
                                log.debug("Incoming outstanding offset difference too large for BitSet encoder (incoming width: {}, old width: {}), will wait before adding these records until the width shrinks (below {})",
                                        width, oldWidth, BitsetEncoder.MAX_LENGTH_ENCODABLE);
                                return false;
//                                break;
                            } else {
                                log.debug("Width was ok {}", width);
                            }
                        }
                    }
                }
            }
        }
        return true;
    }

    /**
     * @return true if the record was taken, false if it was skipped (previously successful)
     */
    private boolean processInbox(final ConsumerRecord<K, V> rec) {
        if (isRecordPreviouslyProcessedSuccessfully(rec)) {
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

            raisePartitionHighestSeen(offset, tp);

            //
            checkPreviousLowWaterMarks(wc);
            checkHighestSucceededSoFar(wc);

            //
            prepareContinuousEncoder(wc);

            //
            processingShards.computeIfAbsent(shardKey, (ignore) -> new ConcurrentSkipListMap<>()).put(offset, wc);

            partitionCommitQueues.computeIfAbsent(tp, (ignore) -> new ConcurrentSkipListMap<>()).put(offset, wc);

            return true;
        }
    }

    private void prepareContinuousEncoder(final WorkContainer<K, V> wc) {
        TopicPartition tp = wc.getTopicPartition();
        if (!partitionContinuousOffsetEncoders.containsKey(tp)) {
            OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(partitionOffsetHighestContinuousSucceeded.get(tp), partitionOffsetHighestSucceeded.get(tp));
            partitionContinuousOffsetEncoders.put(tp, encoder);
        }
    }

    private void checkHighestSucceededSoFar(final WorkContainer<K, V> wc) {
        // previous record must be completed if we've never seen this before
        partitionOffsetHighestSucceeded.putIfAbsent(wc.getTopicPartition(), wc.offset() - 1);
    }

    /**
     * If we've never seen a record for this partition before, it must be our first ever seen record for this partition,
     * which means by definition, it's previous offset is the low water mark.
     */
    private void checkPreviousLowWaterMarks(final WorkContainer<K, V> wc) {
        long previousLowWaterMark = wc.offset() - 1;
        partitionOffsetHighestContinuousSucceeded.putIfAbsent(wc.getTopicPartition(), previousLowWaterMark);
    }

    void raisePartitionHighestSeen(long seenOffset, TopicPartition tp) {
        // rise the high water mark
        Long oldHighestSeen = partitionOffsetHighestSeen.getOrDefault(tp, MISSING_HIGHEST_SEEN);
        if (seenOffset > oldHighestSeen || seenOffset == MISSING_HIGHEST_SEEN) {
            partitionOffsetHighestSeen.put(tp, seenOffset);
        }
    }

    private boolean isRecordPreviouslyProcessedSuccessfully(ConsumerRecord<K, V> rec) {
        long thisRecordsOffset = rec.offset();
        TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
        Set<Long> incompleteOffsets = this.partitionOffsetsIncompleteMetadataPayloads.getOrDefault(tp, new TreeSet<>());
        if (incompleteOffsets.contains(thisRecordsOffset)) {
            // record previously saved as having not been processed
            return false;
        } else {
            Long partitionHighestSeenRecord = partitionOffsetHighestSeen.getOrDefault(tp, MISSING_HIGHEST_SEEN);
            if (thisRecordsOffset <= partitionHighestSeenRecord) {
                // within the range of tracked offsets, but not in incompletes, so must have been previously completed
                return true;
            } else {
                // not in incompletes, and is a higher offset than we've ever seen, as we haven't recorded this far up, so must not have been processed yet
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

    /**
     * @return the maximum amount of work possible
     */
    public <R> List<WorkContainer<K, V>> maybeGetWork() {
        return maybeGetWork(Integer.MAX_VALUE);
    }

    /**
     * Depth first work retrieval.
     */
    public List<WorkContainer<K, V>> maybeGetWork(int requestedMaxWorkToRetrieve) {
        //int minWorkToGetSetting = min(min(requestedMaxWorkToRetrieve, getMaxMessagesToQueue()), getMaxToGoBeyondOffset());
//        int minWorkToGetSetting = min(requestedMaxWorkToRetrieve, getMaxToGoBeyondOffset());
//        int workToGetDelta = requestedMaxWorkToRetrieve - getRecordsOutForProcessing();
        removePartitionFromRecordsAndShardWork();

        int workToGetDelta = requestedMaxWorkToRetrieve;

        // optimise early
        if (workToGetDelta < 1) {
            return UniLists.of();
        }

        // todo this counts all partitions as a whole - this may cause some partitions to starve. need to round robin it?
        int workAvailable = getWorkQueuedInShardsCount();
        int extraNeededFromInboxToSatisfy = requestedMaxWorkToRetrieve - workAvailable;
        log.debug("Requested: {}, workAvailable in shards: {}, will try retrieve from mailbox the delta of: {}",
                requestedMaxWorkToRetrieve, workAvailable, extraNeededFromInboxToSatisfy);
        processInbox(extraNeededFromInboxToSatisfy);

        //
        List<WorkContainer<K, V>> work = new ArrayList<>();

        //
        var it = new LoopingResumingIterator<>(iterationResumePoint, processingShards);

        //
        for (var shard : it) {
            log.trace("Looking for work on shard: {}", shard.getKey());
            if (work.size() >= workToGetDelta) {
                this.iterationResumePoint = Optional.of(shard.getKey());
                log.debug("Work taken is now over max requested, stopping (saving iteration resume point {})", iterationResumePoint);
                break;
            }

            ArrayList<WorkContainer<K, V>> shardWork = new ArrayList<>();
            SortedMap<Long, WorkContainer<K, V>> shardQueue = shard.getValue();

            // then iterate over shardQueue queue
            Set<Map.Entry<Long, WorkContainer<K, V>>> shardQueueEntries = shardQueue.entrySet();
            for (var queueEntry : shardQueueEntries) {
                int taken = work.size() + shardWork.size();
                if (taken >= workToGetDelta) {
                    log.trace("Work taken ({}) exceeds max requested ({})", taken, workToGetDelta);
                    break;
                }

                var workContainer = queueEntry.getValue();
                var topicPartitionKey = workContainer.getTopicPartition();

                {
                    // todo can't just skip, must remove
                    if (checkEpoch(workContainer)) continue;
                }

                // TODO refactor this and the rest of the partition state monitoring code out
                // check we have capacity in offset storage to process more messages
                Boolean allowedMoreRecords = partitionMoreRecordsAllowedToProcess.get(topicPartitionKey);
                // If the record has been previously attempted, it is already represented in the current offset encoding,
                // and may in fact be the message holding up the partition so must be retried
                if (!allowedMoreRecords && workContainer.hasPreviouslyFailed()) {
                    OffsetSimultaneousEncoder offsetSimultaneousEncoder = partitionContinuousOffsetEncoders.get(topicPartitionKey);
                    int encodedSizeEstimate = offsetSimultaneousEncoder.getEncodedSizeEstimate();
                    int metaDataAvailable = getMetadataSpaceAvailablePerPartition();
                    log.warn("Not allowed more records for the partition ({}) that this record ({}) belongs to due to offset " +
                                    "encoding back pressure, continuing on to next container in shard (estimated " +
                                    "required: {}, max available (without tolerance threshold): {})",
                            topicPartitionKey, workContainer.offset(), encodedSizeEstimate, metaDataAvailable);
                    continue;
                }

                boolean alreadySucceeded = !workContainer.isUserFunctionSucceeded();
                if (workContainer.hasDelayPassed(clock) && workContainer.isNotInFlight() && alreadySucceeded) {
                    log.trace("Taking {} as work", workContainer);
                    workContainer.takingAsWork();
                    shardWork.add(workContainer);
                } else {
                    Duration timeInFlight = workContainer.getTimeInFlight();
                    Level level = Level.TRACE;
                    if (toSeconds(timeInFlight) > 1) {
                        level = Level.WARN;
                    }
                    at(log, level).log("Work ({}) still delayed ({}) or is in flight ({}, time in flight: {}), alreadySucceeded? {} can't take...",
                            workContainer, !workContainer.hasDelayPassed(clock), !workContainer.isNotInFlight(), timeInFlight, alreadySucceeded);
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

        checkShardsForProgress();

        log.debug("Got {} records of work. In-flight: {}, Awaiting in commit queues: {}", work.size(), getNumberRecordsOutForProcessing(), getNumberOfEntriesInPartitionQueues());
        numberRecordsOutForProcessing += work.size();

        return work;
    }

    // todo slooooow
    // todo refactor to partition state
    private void checkShardsForProgress() {
        for (var shard : processingShards.entrySet()) {
            for (final Map.Entry<Long, WorkContainer<K, V>> entry : shard.getValue().entrySet()) {
                WorkContainer<K, V> work = entry.getValue();
                long seconds = toSeconds(work.getTimeInFlight());
                if (work.isInFlight() && seconds > 1) {
                    log.warn("Work taking too long {} s : {}", seconds, entry);
                }
            }
        }

    }

    /**
     * Have our partitions been revoked?
     */
    private boolean checkEpoch(final WorkContainer<K, V> workContainer) {
        TopicPartition topicPartitionKey = workContainer.getTopicPartition();

        Integer currentPartitionEpoch = partitionsAssignmentEpochs.get(topicPartitionKey);
        int workEpoch = workContainer.getEpoch();
        if (currentPartitionEpoch != workEpoch) {
            log.warn("Epoch mismatch {} vs {} - were partitions lost? Skipping message - it's already assigned to a different consumer.", workEpoch, currentPartitionEpoch);
            return true;
        }
        return false;
    }

//
//    private int getMaxMessagesToQueue() {
//        //return options.getNumberOfThreads() * options.getLoadingFactor();
//        double rate = successRatePer5Seconds.getRate();
//        int newRatae = (int) rate * 2;
//        int max = Math.max(newRatae, options.getMaxConcurrency() * 10);
//        log.debug("max to queue: {}", max);
//        return max;
////        return options.getNumberOfThreads() * 10;
//    }

//    private final WindowedEventRate successRatePer5Seconds = new WindowedEventRate(5);
//    private final ExponentialMovingAverage successRatePer5SecondsEMA = new ExponentialMovingAverage(0.5);

    public void onSuccess(WorkContainer<K, V> wc) {
        //
//        successRatePer5Seconds.newEvent();
//        successRatePer5SecondsEMA.

        //
        workStateIsDirtyNeedsCommitting.set(true);

        //
        // update as we go
        updateHighestSucceededOffsetSoFar(wc);

        //
        ConsumerRecord<K, V> cr = wc.getCr();
        log.trace("Work success ({}), removing from processing shard queue", wc);
        wc.succeed();

        //
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

        //
        successfulWorkListeners.forEach((c) -> c.accept(wc)); // notify listeners
        numberRecordsOutForProcessing--;

        encodeWorkResult(true, wc);

        // remove work from partition commit queue
        log.trace("Removing {} from partition queue", wc.offset());
        partitionCommitQueues.get(wc.getTopicPartition()).remove(wc.offset());
    }

    public void onResultBatch(final Set<WorkContainer<K, V>> results) {
        //
        if (!results.isEmpty()) {
            onResultUpdatePartitionRecordsBatch(results);
        }

        //
        manageOffsetEncoderSpaceRequirements();


//        // individual
//        for (var work : results) {
//            handleFutureResult(work);
//        }
    }

    protected void handleFutureResult(WorkContainer<K, V> wc) {
        MDC.put("offset", wc.toString());
        TopicPartition tp = wc.getTopicPartition();
        if (wc.getEpoch() < partitionsAssignmentEpochs.get(tp)) {
            log.warn("message assigned from old epoch, ignore: {}", wc);
            return;
        }
        if (wc.isUserFunctionSucceeded()) {
            onSuccess(wc);
        } else {
            onFailure(wc);
        }
        MDC.clear();
    }

    /**
     * Rin algorithms that benefit from seeing chunks of work results
     */
    private void onResultUpdatePartitionRecordsBatch(final Set<WorkContainer<K, V>> results) {
        //
        onResultUpdateHighestContinuousBatch(results);
    }

    private void onResultUpdatePartitionRecords(WorkContainer<K, V> work) {
        TopicPartition tp = work.getTopicPartition();

        if (work.isUserFunctionSucceeded()) {

        } else {
            // no op?

            // this is only recorded in the encoders
            // partitionOffsetsIncompleteMetadataPayloads;
        }
    }

    /**
     * AKA highest committable or low water mark
     *
     * @param workResults must be sorted by offset - partition ordering doesn't matter, as long as we see offsets in
     *                    order
     */
    private void onResultUpdateHighestContinuousBatch(final Set<? extends WorkContainer<K, V>> workResults) {
        HashSet<TopicPartition> partitionsSeenForLogging = new HashSet<>();
        Map<TopicPartition, Long> originalMarks = new HashMap<>();
        Map<TopicPartition, Boolean> partitionNowFormsAContinuousBlock = new HashMap<>();
        for (final WorkContainer<K, V> work : workResults) {
            if (checkEpoch(work)) continue;

            TopicPartition tp = work.getTopicPartition();

            // guard against old epoch messages
            // TODO don't do this as upon revoke we try to commit this work. Maybe the commit attempt needs to mark the epoch as discarded, and in that case we should do this drop
//            if (work.getEpoch() < parittionsAssignmentEpochs.get(tp)) {
//                log.warn("Message assigned from old epoch, ignore: {}", work);
//                continue;
//            }


            long thisOffset = work.getCr().offset();


            // this offset has already been scanned as a part of a high range batch, so skip (we already know the highest continuous block incorporates this offset)
            Long previousHighestContinuous = partitionOffsetHighestContinuousSucceeded.get(tp);
            if (thisOffset <= previousHighestContinuous) {
//                     sanity? by definition it must be higher
//                    throw new InternalRuntimeError(msg("Unexpected new offset {} lower than low water mark {}", thisOffset, previousHighestContinuous));
                // things can be racey, so this can happen, if so, just continue
                log.debug("Completed offset {} lower than current highest continuous offset {} - must have been completed while previous continuous blocks were being examined", thisOffset, previousHighestContinuous);
//                continue; - can't skip #handleResult
            } else {


                // We already know this partition's continuous range has been broken, no point checking
                Boolean partitionSoFarIsContinuous = partitionNowFormsAContinuousBlock.get(tp);
                if (partitionSoFarIsContinuous != null && !partitionSoFarIsContinuous) {
                    // previously we found non continuous block so we can skip
//                    continue; // to next record - can't skip #handleResult
                } else {
                    // we can't know, we have to keep digging


                    boolean thisOffsetIsFailed = !work.isUserFunctionSucceeded();
                    partitionsSeenForLogging.add(tp);

                    if (thisOffsetIsFailed) {
                        // simpler path
                        // work isn't successful. Is this the first? Is there a gap previously? Perhaps the gap doesn't exist (skipped offsets in partition)
                        Boolean previouslyContinuous = partitionNowFormsAContinuousBlock.get(tp);
                        partitionNowFormsAContinuousBlock.put(tp, false); // this partitions continuous block
                    } else {

                        // does it form a new continuous block?

                        // queue this offset belongs to
                        NavigableMap<Long, WorkContainer<K, V>> commitQueue = partitionCommitQueues.get(tp);

                        boolean continuous = true;
                        if (thisOffset != previousHighestContinuous + 1) {
                            // do the entries in the gap exist in our partition queue? or are they skipped in the source log?
                            long rangeBase = (previousHighestContinuous < 0) ? 0 : previousHighestContinuous + 1;
                            Range offSetRangeToCheck = new Range(rangeBase, thisOffset);
                            log.trace("Gap detected between {} and {}", rangeBase, thisOffset);
                            for (var offsetToCheck : offSetRangeToCheck) {
                                WorkContainer<K, V> workToExamine = commitQueue.get((long) offsetToCheck);
                                if (workToExamine != null) {
                                    if (!workToExamine.isUserFunctionSucceeded()) {
                                        log.trace("Record exists {} but is incomplete - breaks continuity finish early", workToExamine);
                                        partitionNowFormsAContinuousBlock.put(tp, false);
                                        continuous = false;
                                        break;
                                    } else if (workToExamine.isUserFunctionSucceeded() && !workToExamine.isNotInFlight()) {
                                        log.trace("Work {} comparing to succeeded work still in flight: {} (but not part of this batch)", work.offset(), workToExamine);
//                                        continue;  - can't skip #handleResult
                                    } else {
                                        // counts as continuous, just isn't in this batch - previously successful but there used to be gaps
                                        log.trace("Work not in batch, but seen now in commitQueue as succeeded {}", workToExamine);
                                    }
                                } else {
                                    // offset doesn't exist in commit queue, can assume doesn't exist in source, or is completed
                                    log.trace("Work offset {} checking against offset {} missing from commit queue, assuming doesn't exist in source", work.offset(), offsetToCheck);
                                }
                            }

                        }
                        if (continuous) {
                            partitionNowFormsAContinuousBlock.put(tp, true);
                            if (!originalMarks.containsKey(tp)) {
                                Long previousOffset = partitionOffsetHighestContinuousSucceeded.get(tp);
                                originalMarks.put(tp, previousOffset);
                            }
                            partitionOffsetHighestContinuousSucceeded.put(tp, thisOffset);
                        } else {
                            partitionNowFormsAContinuousBlock.put(tp, false);
//                        Long old = partitionOffsetHighestContinuousCompleted.get(tp);
                        }
//                    else {
//                        // easy, yes it's continuous, as there's no gap from previous highest
//                        partitionNowFormsAContinuousBlock.put(tp, true);
//                        partitionOffsetHighestContinuousCompleted.put(tp, thisOffset);
//                    }
                    }
                }
            }

            //
            {
                handleFutureResult(work);
            }

        }
        for (final TopicPartition tp : partitionsSeenForLogging) {
            Long oldOffset = originalMarks.get(tp);
            Long newOffset = partitionOffsetHighestContinuousSucceeded.get(tp);
            log.debug("Low water mark (highest continuous completed) for partition {} moved from {} to {}, highest succeeded {}",
                    tp, oldOffset, newOffset, partitionOffsetHighestSucceeded.get(tp));
        }
    }

    /**
     * Update highest Succeeded seen so far
     */
    private void updateHighestSucceededOffsetSoFar(final WorkContainer<K, V> work) {
        //
        TopicPartition tp = work.getTopicPartition();
        Long highestCompleted = partitionOffsetHighestSucceeded.getOrDefault(tp, -1L);
        long thisOffset = work.getCr().offset();
        if (thisOffset > highestCompleted) {
            log.trace("Updating highest completed - was: {} now: {}", highestCompleted, thisOffset);
            partitionOffsetHighestSucceeded.put(tp, thisOffset);
        }
    }

    /**
     * Make encoders add new work result to their encodings
     * <p>
     * todo refactor to offset manager?
     */
    private void encodeWorkResult(final boolean offsetComplete, final WorkContainer<K, V> wc) {
        TopicPartition tp = wc.getTopicPartition();
        long lowWaterMark = partitionOffsetHighestContinuousSucceeded.get(tp);
        Long highestCompleted = partitionOffsetHighestSucceeded.get(tp);

        long nextExpectedOffsetFromBroker = lowWaterMark + 1;

        OffsetSimultaneousEncoder offsetSimultaneousEncoder = partitionContinuousOffsetEncoders.get(tp);

        long offset = wc.offset();

        // give encoders chance to truncate
        offsetSimultaneousEncoder.maybeReinitialise(nextExpectedOffsetFromBroker, highestCompleted);

        if (offset <= nextExpectedOffsetFromBroker) {
            // skip - nothing to encode
            return;
        }

        long relativeOffset = offset - nextExpectedOffsetFromBroker;
        if (relativeOffset < 0) {
//            throw new InternalRuntimeError(msg("Relative offset negative {}", relativeOffset));
            log.trace("Offset {} now below low water mark {}, no need to encode", offset, lowWaterMark);
            return;
        }

        if (offsetComplete)
            offsetSimultaneousEncoder.encodeCompleteOffset(nextExpectedOffsetFromBroker, relativeOffset, highestCompleted);
        else
            offsetSimultaneousEncoder.encodeIncompleteOffset(nextExpectedOffsetFromBroker, relativeOffset, highestCompleted);
    }

    /**
     * Todo: Does this need to be run per message of a a result set? or only once the batch has been finished, once per
     * partition? As we can't change anything mid flight - only for the next round
     */
    private void manageOffsetEncoderSpaceRequirements() {
        int perPartition = getMetadataSpaceAvailablePerPartition();
        double tolerance = 0.7; // 90%

        boolean anyPartitionsAreHalted = false;

        // for each encoded partition so far, check if we're within tolerance of max space
        for (final Map.Entry<TopicPartition, OffsetSimultaneousEncoder> entry : partitionContinuousOffsetEncoders.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetSimultaneousEncoder encoder = entry.getValue();
            int encodedSize = encoder.getEncodedSizeEstimate();

            int allowed = (int) (perPartition * tolerance);

            boolean moreMessagesAreAllowed = allowed > encodedSize;

            boolean previousMessagesAllowedState = partitionMoreRecordsAllowedToProcess.get(tp);
            // update partition with tolerance threshold crossed status
            partitionMoreRecordsAllowedToProcess.put(tp, moreMessagesAreAllowed);
            if (!moreMessagesAreAllowed && previousMessagesAllowedState) {
                anyPartitionsAreHalted = true;
                log.debug(msg("Back-pressure for {} activated, no more messages allowed, best encoder {} needs {} which is more than " +
                                "calculated restricted space of {} (max: {}, tolerance {}%). Messages will be allowed again once messages " +
                                "complete and encoding space required shrinks.",
                        tp, encoder.getSmallestCodec(), encodedSize, allowed, perPartition, tolerance * 100));
            } else if (moreMessagesAreAllowed && !previousMessagesAllowedState) {
                log.trace("Partition is now unblocked, needed {}, allowed {}", encodedSize, allowed);
            } else if (!moreMessagesAreAllowed && !previousMessagesAllowedState) {
                log.trace("Partition {} still blocked for new message processing", tp);
            }

            boolean offsetEncodingAlreadyWontFitAtAll = encodedSize > perPartition;
            if (offsetEncodingAlreadyWontFitAtAll) {
                log.warn("Despite attempts, current offset encoding requirements are now above what will fit. Offset encoding " +
                        "will be dropped for this round, but no more messages for this partition will be attempted until " +
                        "messages complete successfully and the offset encoding space required shrinks again.");
                log.warn(msg("Back-pressure for {} activated, no more messages allowed, best encoder {} needs {} which is more than calculated " +
                                "restricted space of {} (max: {}, tolerance {}%). Messages will be allowed again once messages complete and encoding " +
                                "space required shrinks.",
                        tp, encoder.getSmallestCodec(), encodedSize, allowed, perPartition, tolerance * 100));
            }

        }
        if (anyPartitionsAreHalted) {
            log.debug("Some partitions were halted");
        }
    }

    private int getMetadataSpaceAvailablePerPartition() {
        int defaultMaxMetadataSize = OffsetMapCodecManager.DefaultMaxMetadataSize;
        // TODO what else is the overhead in b64 encoding?
        int maxMetadataSize = defaultMaxMetadataSize - OffsetEncoding.standardOverhead;
        if (numberOfAssignedPartitions == 0) {
            // no partitions assigned - all available
            return maxMetadataSize;
//            throw new InternalRuntimeError("Nothing assigned");
        }
        int perPartition = maxMetadataSize / numberOfAssignedPartitions;
        return perPartition;
    }

    public void onFailure(WorkContainer<K, V> wc) {
        //
        wc.fail(clock);

        //
        putBack(wc);

        //
        encodeWorkResult(false, wc);
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
        if (!isDirty()) {
            // nothing to commit
            return UniMaps.of();
        }

        Map<TopicPartition, OffsetAndMetadata> offsetMetadataToCommit = new HashMap<>();
        int totalPartitionQueueSizeForLogging = 0;
        int removed = 0;
        log.trace("Scanning for in order in-flight work that has completed...");
        int totalOffsetMetaCharacterLengthUsed = 0;
        for (final var partitionQueueEntry : partitionCommitQueues.entrySet()) {
            //
            totalPartitionQueueSizeForLogging += partitionQueueEntry.getValue().size();
            var partitionQueue = partitionQueueEntry.getValue();

            var workToRemove = new LinkedList<WorkContainer<K, V>>();
            var incompleteOffsets = new LinkedHashSet<Long>();  // we only need to know the full incompletes when we do this scan, so find them only now, and discard

            //
            long lowWaterMark = -1;
            // can't commit this offset or beyond, as this is the latest offset that is incomplete
            // i.e. only commit offsets that come before the current one, and stop looking for more
            boolean iteratedBeyondLowWaterMarkBeingLowestCommittableOffset = false;

            //
            TopicPartition topicPartitionKey = partitionQueueEntry.getKey();
            log.trace("Starting scan of partition: {}", topicPartitionKey);
            Long firstIncomplete = null;
            Long baseOffset = partitionOffsetHighestContinuousSucceeded.get(topicPartitionKey);
            for (final var offsetAndItsWorkContainer : partitionQueue.entrySet()) {
                // ordered iteration via offset keys thanks to the tree-map
                WorkContainer<K, V> work = offsetAndItsWorkContainer.getValue();
                boolean inFlight = !work.isNotInFlight(); // check is part of this mailbox set / not in flight
                if (inFlight) {
                    log.trace("Skipping comparing to work still in flight: {}", work);
                    continue;
                }
                long offset = work.getCr().offset();
                boolean workCompleted = work.isUserFunctionComplete();
                if (workCompleted) {
                    if (work.isUserFunctionSucceeded() && !iteratedBeyondLowWaterMarkBeingLowestCommittableOffset) {
                        log.trace("Found offset candidate ({}) to add to offset commit map", work);
                        workToRemove.add(work);
                        // as in flights are processed in order, this will keep getting overwritten with the highest offset available
                        // current offset is the highest successful offset, so commit +1 - offset to be committed is defined as the offset of the next expected message to be read
                        long offsetOfNextExpectedMessageAkaHighestCommittableAkaLowWaterMark = offset + 1;
                        OffsetAndMetadata offsetData = new OffsetAndMetadata(offsetOfNextExpectedMessageAkaHighestCommittableAkaLowWaterMark);
                        offsetMetadataToCommit.put(topicPartitionKey, offsetData);
                    } else if (work.isUserFunctionSucceeded() && iteratedBeyondLowWaterMarkBeingLowestCommittableOffset) {
                        // todo lookup the low water mark and include here
                        log.trace("Offset {} is complete and succeeded, but we've iterated past the lowest committable offset ({}). Will mark as complete in the offset map.",
                                work.getCr().offset(), lowWaterMark);
                        // no-op - offset map is only for not succeeded or completed offsets
//                        // mark as complete complete so remove from work
//                        workToRemove.add(work);
                    } else {
                        log.trace("Offset {} is complete, but failed processing. Will track in offset map as not complete. Can't do normal offset commit past this point.", work.getCr().offset());
                        iteratedBeyondLowWaterMarkBeingLowestCommittableOffset = true;
                        incompleteOffsets.add(offset);
                        if (firstIncomplete == null)
                            firstIncomplete = offset;
                    }
                } else {
                    lowWaterMark = work.offset();

                    // work not complete - either successfully or unsuccessfully
                    iteratedBeyondLowWaterMarkBeingLowestCommittableOffset = true;
                    log.trace("Offset ({}) is incomplete, holding up the queue ({}) of size {}.",
                            work.getCr().offset(),
                            partitionQueueEntry.getKey(),
                            partitionQueueEntry.getValue().size());
                    incompleteOffsets.add(offset);
                    if (firstIncomplete == null)
                        firstIncomplete = offset;
                }
            }

//            {
//                OffsetSimultaneousEncoder precomputed = partitionContinuousOffsetEncoders.get(topicPartitionKey);
//                byte[] bytes = new byte[0];
//                try {
//                    Long currentHighestCompleted = partitionOffsetHighestSucceeded.get(topicPartitionKey) + 1;
//                    if (firstIncomplete != null && baseOffset != firstIncomplete - 1) {
//                        log.warn("inconsistent base new vs old {} {} diff: {}", baseOffset, firstIncomplete, firstIncomplete - baseOffset);
//                        if (baseOffset > firstIncomplete) {
//                            log.warn("batch computed is higher than this scan??");
//                        }
//                    }
//                    Long highestSeen = partitionOffsetHighestSeen.get(topicPartitionKey); // we don't expect these to be different
//                    if (currentHighestCompleted != highestSeen) {
//                        log.debug("New system upper end vs old system {} {} (delta: {})", currentHighestCompleted, highestSeen, highestSeen - currentHighestCompleted);
//                    }
//
////                    precomputed.runOverIncompletes(incompleteOffsets, baseOffset, currentHighestCompleted);
//                    precomputed.serializeAllEncoders();
//
//                    // make this a field instead - has no state?
//                    OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this, this.consumerMgr);
//                    String smallestMetadataPacked = om.makeOffsetMetadataPayload(precomputed);
//
//                    totalOffsetMetaCharacterLengthUsed += smallestMetadataPacked.length();
//                    log.debug("comparisonOffsetPayloadString :{}:", smallestMetadataPacked);
//                    OffsetAndMetadata offsetWithExtraMap = new OffsetAndMetadata(baseOffset + 1, smallestMetadataPacked);
//                    offsetMetadataToCommit.put(topicPartitionKey, offsetWithExtraMap);
//                } catch (EncodingNotSupportedException e) {
//                    e.printStackTrace();
//                }

//            OffsetAndMetadata offsetAndMetadata = offsetMetadataToCommit.get(topicPartitionKey);
//            {
//                int offsetMetaPayloadSpaceUsed = getTotalOffsetMetaCharacterLength(offsetMetadataToCommit, totalOffsetMetaCharacterLengthUsed, incompleteOffsets, topicPartitionKey);
//                totalOffsetMetaCharacterLengthUsed += offsetMetaPayloadSpaceUsed;
//            }

            if (remove) {
                removed += workToRemove.size();
                for (var workContainer : workToRemove) {
                    var offset = workContainer.getCr().offset();
                    partitionQueue.remove(offset);
                }
            }

        }

        maybeStripOffsetPayload(offsetMetadataToCommit, totalOffsetMetaCharacterLengthUsed);

        log.debug("Scan finished, {} were in flight, {} completed offsets removed, coalesced to {} offset(s) ({}) to be committed",
                totalPartitionQueueSizeForLogging, removed, offsetMetadataToCommit.size(), offsetMetadataToCommit);
        return offsetMetadataToCommit;
    }

//    private int getTotalOffsetMetaCharacterLength(final Map<TopicPartition, OffsetAndMetadata> perPartitionNextExpectedOffset, int totalOffsetMetaCharacterLength, final LinkedHashSet<Long> incompleteOffsets, final TopicPartition topicPartitionKey) {
//        // offset map building
//        // Get final offset data, build the the offset map, and replace it in our map of offset data to send
//        // TODO potential optimisation: store/compare the current incomplete offsets to the last committed ones, to know if this step is needed or not (new progress has been made) - isdirty?
//        if (!incompleteOffsets.isEmpty()) {
//            long offsetOfNextExpectedMessage;
//            OffsetAndMetadata finalOffsetOnly = perPartitionNextExpectedOffset.get(topicPartitionKey);
//            if (finalOffsetOnly == null) {
//                // no new low water mark to commit, so use the last one again
//                offsetOfNextExpectedMessage = incompleteOffsets.iterator().next(); // first element
//            } else {
//                offsetOfNextExpectedMessage = finalOffsetOnly.offset();
//            }
//
//            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this, this.consumerMgr);
//            try {
//                // TODO change from offsetOfNextExpectedMessage to getting the pre computed one from offsetOfNextExpectedMessage
//                Long highestCompletedOffset = partitionOffsetHighestSucceeded.get(topicPartitionKey);
//                if (highestCompletedOffset == null) {
//                    log.error("What now?");
//                }
//                // encode
//                String offsetMapPayload = om.makeOffsetMetadataPayload(offsetOfNextExpectedMessage, topicPartitionKey, incompleteOffsets);
//                totalOffsetMetaCharacterLength += offsetMapPayload.length();
//                OffsetAndMetadata offsetWithExtraMap = new OffsetAndMetadata(offsetOfNextExpectedMessage, offsetMapPayload);
//                perPartitionNextExpectedOffset.put(topicPartitionKey, offsetWithExtraMap);
//            } catch (EncodingNotSupportedException e) {
//                log.warn("No encodings could be used to encode the offset map, skipping. Warning: messages might be replayed on rebalance", e);
////                backoffer.onFailure();
//            }
//        }
//        return totalOffsetMetaCharacterLength;
//    }

    /**
     * Once all the offset maps have been calculated, check if they're too big, and if so, remove all of them.
     * <p>
     * Implication of this is that if the system has to recover from this offset, then it will have to replay all the
     * messages that were otherwise complete.
     * <p>
     * Must be thread safe.
     *
     * @see OffsetMapCodecManager#DefaultMaxMetadataSize
     */
    private void maybeStripOffsetPayload(Map<TopicPartition, OffsetAndMetadata> offsetsToSend,
                                         int totalOffsetMetaCharacterLength) {
        // TODO: Potential optimisation: if max metadata size is shared across partitions, the limit used could be relative to the number of
        //  partitions assigned to this consumer. In which case, we could derive the limit for the number of downloaded but not committed
        //  offsets, from this max over some estimate. This way we limit the possibility of hitting the hard limit imposed in the protocol, thus
        //  retaining the offset map feature, at the cost of potential performance by hitting a soft maximum in our uncommitted concurrent processing.
        if (totalOffsetMetaCharacterLength > OffsetMapCodecManager.DefaultMaxMetadataSize) {
            log.warn("Offset map data too large (size: {}) to fit in metadata payload - stripping offset map out. " +
                            "See kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize = 4096",
                    totalOffsetMetaCharacterLength);
            // strip all payloads
            // todo iteratively strip the largest payloads until we're under the limit
            int totalSizeEstimates = 0;
            for (var entry : offsetsToSend.entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetSimultaneousEncoder offsetSimultaneousEncoder = partitionContinuousOffsetEncoders.get(tp);
                int encodedSizeEstimate = offsetSimultaneousEncoder.getEncodedSizeEstimate();
                log.debug("Estimate for {} {}", tp, encodedSizeEstimate);
                totalSizeEstimates += encodedSizeEstimate;
                OffsetAndMetadata v = entry.getValue();
                OffsetAndMetadata stripped = new OffsetAndMetadata(v.offset()); // meta data gone
                offsetsToSend.replace(tp, stripped);
            }
            log.debug("Total estimate for all partitions {}", totalSizeEstimates);
//            backoffer.onFailure();
        } else if (totalOffsetMetaCharacterLength != 0) {
            log.debug("Offset map small enough to fit in payload: {} (max: {})", totalOffsetMetaCharacterLength, OffsetMapCodecManager.DefaultMaxMetadataSize);
//            backoffer.onSuccess();
        }
    }

    /**
     * Called after a successful commit off offsets
     */
    public void onOffsetCommitSuccess(Map<TopicPartition, OffsetAndMetadata> offsetsCommitted) {
        truncateOffsetsIncompleteMetadataPayloads(offsetsCommitted);
        workStateIsDirtyNeedsCommitting.set(false);
    }

    /**
     * Truncate our tracked offsets as a commit was successful, so the low water mark rises, and we don't need to track
     * as much anymore.
     * <p>
     * When commits are made to broker, we can throw away all the individually tracked offsets lower than the base
     * offset which is in the commit.
     */
    private void truncateOffsetsIncompleteMetadataPayloads(
            final Map<TopicPartition, OffsetAndMetadata> offsetsCommitted) {
        // partitionOffsetHighWaterMarks this will get overwritten in due course
        offsetsCommitted.forEach((tp, meta) -> {
            Set<Long> incompleteOffsets = partitionOffsetsIncompleteMetadataPayloads.get(tp);
            boolean trackedOffsetsForThisPartitionExist = incompleteOffsets != null;
            if (trackedOffsetsForThisPartitionExist) {
                long newLowWaterMark = meta.offset();
                incompleteOffsets.removeIf(offset -> offset < newLowWaterMark);
            }
        });
    }

    public boolean shouldThrottle() {
        return isSufficientlyLoaded();
    }

    /**
     * @return true if there's enough messages downloaded from the broker already to satisfy the pipeline, false if more
     *         should be downloaded (or pipelined in the Consumer)
     */
    boolean isSufficientlyLoaded() {
//        int total = getTotalWorkWaitingProcessing();
//        int inPartitions = getNumberOfEntriesInPartitionQueues();
//        int maxBeyondOffset = getMaxToGoBeyondOffset();
//        boolean loadedEnoughInPipeline = total > maxBeyondOffset * loadingFactor;
//        boolean overMaxUncommitted = inPartitions >= maxBeyondOffset;
//        boolean remainingIsSufficient = loadedEnoughInPipeline || overMaxUncommitted;
////        if (remainingIsSufficient) {
//        log.debug("isSufficientlyLoaded? loadedEnoughInPipeline {} || overMaxUncommitted {}", loadedEnoughInPipeline, overMaxUncommitted);
////        }
//        return remainingIsSufficient;
//
//        return !workInbox.isEmpty();

        return getWorkQueuedInMailboxCount() > options.getMaxConcurrency() * getLoadingFactor();
    }

    private int getLoadingFactor() {
        return dynamicLoadFactor.getCurrentFactor();
    }

    // TODO effeciency issues
    public boolean workIsWaitingToBeCompletedSuccessfully() {
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

    /**
     * fastish
     *
     * @return
     */
    public boolean hasWorkInCommitQueues() {
        for (var e : this.partitionCommitQueues.entrySet()) {
            if (!e.getValue().isEmpty())
                return true;
        }
        return false;
    }

    public Map<TopicPartition, OffsetAndMetadata> serialiseEncoders() {
        if (!isDirty()) {
            log.trace("Nothing to commit, work state is clean");
            return UniMaps.of();
        }

        Map<TopicPartition, OffsetAndMetadata> offsetMetadataToCommit = new HashMap<>();
//        int totalPartitionQueueSizeForLogging = 0;
        int totalOffsetMetaCharacterLengthUsed = 0;

        for (final Map.Entry<TopicPartition, OffsetSimultaneousEncoder> tpEncoder : partitionContinuousOffsetEncoders.entrySet()) {
            TopicPartition topicPartitionKey = tpEncoder.getKey();
            OffsetSimultaneousEncoder precomputed = tpEncoder.getValue();
            log.trace("Serialising available encoders for {} using {}", topicPartitionKey, precomputed);
            try {
                precomputed.serializeAllEncoders();

                OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this, this.consumerMgr);
                String smallestMetadataPacked = om.makeOffsetMetadataPayload(precomputed);

                totalOffsetMetaCharacterLengthUsed += smallestMetadataPacked.length();

                long nextExpectedOffsetFromBroker = precomputed.getBaseOffset();
                OffsetAndMetadata offsetWithExtraMap = new OffsetAndMetadata(nextExpectedOffsetFromBroker, smallestMetadataPacked);
                offsetMetadataToCommit.put(topicPartitionKey, offsetWithExtraMap);
            } catch (EncodingNotSupportedException e) {
                log.warn("No encodings could be used to encode the offset map, skipping. Warning: messages might be replayed on rebalance", e);
//                backoffer.onFailure();
            }
        }

        maybeStripOffsetPayload(offsetMetadataToCommit, totalOffsetMetaCharacterLengthUsed);

        log.debug("Scan finished, coalesced to {} offset(s) ({}) to be committed",
                offsetMetadataToCommit.size(), offsetMetadataToCommit);

        return offsetMetadataToCommit;
    }

}
