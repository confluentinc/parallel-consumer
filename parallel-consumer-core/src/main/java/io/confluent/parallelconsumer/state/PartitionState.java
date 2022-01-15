package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.LinkedList;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import static lombok.AccessLevel.*;

@Slf4j
public class PartitionState<K, V> {

    @Getter
    private final TopicPartition tp;

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
    // visible for testing
    // todo should be tracked live, as we know when the state of work containers flips - i.e. they are continuously tracked
    // this is derived from partitionCommitQueues WorkContainer states
    // todo remove setter - leaky abstraction, shouldn't be needed
    @Setter
    private Set<Long> incompleteOffsets;

    public Set<Long> getIncompleteOffsets() {
        return Collections.unmodifiableSet(incompleteOffsets);
    }

    /**
     * The highest seen offset for a partition.
     * <p>
     * Starts off as null - no data
     */
    // visible for testing
    @NonNull
    @Getter(PUBLIC)
    private Long offsetHighestSeen;

    /**
     * Highest offset which has completed successfully ("succeeded").
     */
    @Getter(PUBLIC)
    @Setter(PRIVATE)
    private long offsetHighestSucceeded = -1L;

    /**
     * If true, more messages are allowed to process for this partition.
     * <p>
     * If false, we have calculated that we can't record any more offsets for this partition, as our best performing
     * encoder requires nearly as much space is available for this partitions allocation of the maximum offset metadata
     * size.
     * <p>
     * Default (missing elements) is true - more messages can be processed.
     * <p>
     * AKA high water mark (which is a deprecated description).
     *
     * @see OffsetMapCodecManager#DefaultMaxMetadataSize
     */
    @Getter(PACKAGE)
    @Setter(PACKAGE)
    private boolean allowedMoreRecords = true;

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
    private final NavigableMap<Long, WorkContainer<K, V>> commitQueue = new ConcurrentSkipListMap<>();

    NavigableMap<Long, WorkContainer<K, V>> getCommitQueue() {
        return Collections.unmodifiableNavigableMap(commitQueue);
    }

    public PartitionState(TopicPartition tp, OffsetMapCodecManager.HighestOffsetAndIncompletes incompletes) {
        this.tp = tp;
        this.incompleteOffsets = incompletes.getIncompleteOffsets();
        this.offsetHighestSeen = incompletes.getHighestSeenOffset();
    }

    public void maybeRaiseHighestSeenOffset(final long highestSeen) {
        // rise the high water mark
        Long oldHighestSeen = this.offsetHighestSeen;
        if (oldHighestSeen == null || highestSeen >= oldHighestSeen) {
            log.trace("Updating highest seen - was: {} now: {}", offsetHighestSeen, highestSeen);
            offsetHighestSeen = highestSeen;
        }
    }

    /**
     * Removes all offsets that fall below the new low water mark.
     */
    public void truncateOffsets(final long nextExpectedOffset) {
        incompleteOffsets.removeIf(offset -> offset < nextExpectedOffset);
    }

    public void onOffsetCommitSuccess(final OffsetAndMetadata committed) {
        long nextExpectedOffset = committed.offset();
        truncateOffsets(nextExpectedOffset);
    }

    public boolean isRecordPreviouslyCompleted(final ConsumerRecord<K, V> rec) {
        boolean previouslyProcessed;
        long offset = rec.offset();
        if (incompleteOffsets.contains(offset)) {
            // record previously saved as not being completed, can exit early
            previouslyProcessed = false;
        } else {
            Long offsetHighWaterMark = offsetHighestSeen;
            // within the range of tracked offsets, so must have been previously completed
            // we haven't recorded this far up, so must not have been processed yet
            previouslyProcessed = offsetHighWaterMark != null && offset <= offsetHighWaterMark;
        }
        return previouslyProcessed;
    }

    public boolean hasWorkInCommitQueue() {
        return !commitQueue.isEmpty();
    }

    public boolean hasWorkThatNeedsCommitting() {
        return commitQueue.values().parallelStream().anyMatch(x -> x.isUserFunctionSucceeded());
    }

    public int getCommitQueueSize() {
        return commitQueue.size();
    }

    public void onSuccess(WorkContainer<K, V> work) {
        updateHighestSucceededOffsetSoFar(work);
    }

    /**
     * Update highest Succeeded seen so far
     */
    private void updateHighestSucceededOffsetSoFar(WorkContainer<K, V> work) {
        long highestSucceeded = getOffsetHighestSucceeded();
        long thisOffset = work.offset();
        if (thisOffset > highestSucceeded) {
            log.trace("Updating highest completed - was: {} now: {}", highestSucceeded, thisOffset);
            setOffsetHighestSucceeded(thisOffset);
        }
    }

    public void addWorkContainer(WorkContainer<K, V> wc) {
        maybeRaiseHighestSeenOffset(wc.offset());
        NavigableMap<Long, WorkContainer<K, V>> queue = this.commitQueue;
        queue.put(wc.offset(), wc);
    }

    public void remove(LinkedList<WorkContainer<K, V>> workToRemove) {
        for (var workContainer : workToRemove) {
            var offset = workContainer.getCr().offset();
            this.commitQueue.remove(offset);
        }
    }

    /**
     * Has this partition been removed? No.
     *
     * @return by definition false in this implementation
     */
    public boolean isRemoved() {
        return false;
    }

}
