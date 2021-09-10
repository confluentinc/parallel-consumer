package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.OffsetMapCodecManager;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class PartitionState<K, V> {

    @Getter
    private final TopicPartition tp;

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
    // todo should be tracked live, as we know when the state of work containers flips - i.e. they are continuously tracked
    // todo make private
    // todo make private and final - needs to move to constructor, see #onPartitionsAssigned and constructor usage
    // this is derived from partitionCommitQueues WorkContainer states
    @Setter
    @Getter
    private Set<Long> partitionIncompleteOffsets;

    // visible for testing
    static final long MISSING_HIGH_WATER_MARK = -1L;

    // visible for testing
    /**
     * The highest seen offset for a partition.
     * <p>
     * Starts off as null - no data
     */
    // TODO rename to partitionOffsetHighestSeen - cascading rename
    // todo make private - package
    // todo change to optional instead of -1 - overkill?
    @NonNull
    @Getter(AccessLevel.PUBLIC)
    private Long partitionOffsetHighWaterMarks;// = MISSING_HIGH_WATER_MARK;

    /**
     * Highest offset which has completed.
     */
    // todo not used - future feature? only used be continuous encoder branches
    // todo null valid? change to option?
    // Long partitionOffsetHighestSucceeded;

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
    // todo get/set make private
    // todo rename more eloquently - isAllowedMoreRecords?
    @Getter(AccessLevel.PACKAGE)
    @Setter(AccessLevel.PACKAGE)
    private boolean allowedMoreRecords = true;

    /**
     * Record the generations of partition assignment, for fencing off invalid work
     */
    // todo make private
    @Getter(AccessLevel.PACKAGE)
    private int partitionsAssignmentEpochs = 0;

    /**
     * Map of offsets to WorkUnits.
     * <p>
     * Need to record globally consumed records, to ensure correct offset order committal. Cannot rely on incrementally
     * advancing offsets, as this isn't a guarantee of kafka's.
     * <p>
     * Concurrent because either the broker poller thread or the control thread may be requesting offset to commit
     * ({@link #findCompletedEligibleOffsetsAndRemove})
     *
     * @see #findCompletedEligibleOffsetsAndRemove
     */
    // todo rename commitQueue
    // todo make private
    // todo remove state access
    @Getter
    private final NavigableMap<Long, WorkContainer<K, V>> partitionCommitQueues = new ConcurrentSkipListMap<>();

    public PartitionState(TopicPartition tp, OffsetMapCodecManager.HighestOffsetAndIncompletes incompletes) {
        this.tp = tp;
        this.partitionIncompleteOffsets = incompletes.getIncompleteOffsets();
        this.partitionOffsetHighWaterMarks = incompletes.getHighestSeenOffset();
    }

    // todo does this make sense to keep here if state is being rebuilt
    public void incrementPartitionAssignmentEpoch() {
        partitionsAssignmentEpochs++;
    }

    public void risePartitionHighWaterMark(final long highWater) {
        // rise the high water mark
        Long oldHighWaterMark = this.partitionOffsetHighWaterMarks;
        if (oldHighWaterMark == null || highWater >= oldHighWaterMark) {
            partitionOffsetHighWaterMarks = highWater;
        }
    }

    /**
     * Removes all offsets that fall below the new low water mark.
     * @param newLowWaterMark // todo rename variable from newLowWaterMark
     */
    public void truncateOffsets(final long newLowWaterMark) {
        partitionIncompleteOffsets.removeIf(offset -> offset < newLowWaterMark);
    }

    public void onOffsetCommitSuccess(final OffsetAndMetadata meta) {
        long newLowWaterMark = meta.offset();
        truncateOffsets(newLowWaterMark);
    }

    public <K, V> boolean isRecordPreviouslyProcessed(final ConsumerRecord<K, V> rec) {
        Set<Long> incompleteOffsets = partitionIncompleteOffsets;
        //Set<Long> incompleteOffsets = this.partitionIncompleteOffsets.getOrDefault(tp, new TreeSet<>());
        boolean previouslyProcessed;
        long offset = rec.offset();
        if (incompleteOffsets.contains(offset)) {
            // record previously saved as having not been processed, can exit early
            previouslyProcessed = false;
        } else {
            Long offsetHighWaterMark = partitionOffsetHighWaterMarks;
//            Long offsetHighWaterMark = partitionOffsetHighWaterMarks.getOrDefault(tp, MISSING_HIGH_WATER_MARK);
            // within the range of tracked offsets, so must have been previously completed
            // we haven't recorded this far up, so must not have been processed yet
            previouslyProcessed = offsetHighWaterMark != null && offset <= offsetHighWaterMark;
        }
        return previouslyProcessed;
    }

    public boolean hasWorkInCommitQueue() {
        return !partitionCommitQueues.isEmpty();
    }

    public int getCommitQueueSize() {
        return partitionCommitQueues.size();
    }

}
