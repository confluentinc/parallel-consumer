package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.offsets.NoEncodingPossibleException;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.DefaultMaxMetadataSize;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static lombok.AccessLevel.PACKAGE;
import static lombok.AccessLevel.PUBLIC;

@Slf4j
public class PartitionState<K, V> {

    @Getter
    private final TopicPartition tp;

    /**
     * Offset data beyond the highest committable offset, which haven't totally succeeded.
     * <p>
     * This is independent of the actual queued {@link WorkContainer}s. This is because to start with, data about
     * incomplete offsets come from the encoded metadata payload that gets committed along with the highest committable
     * offset ({@link #getOffsetHighestSequentialSucceeded()}). They are not always in sync.
     * <p>
     * TreeSet so we can always get the lowest offset.
     */
    // todo why concurrent - doesn't need it?
    private final TreeSet<Long> incompleteOffsets;

    /**
     * @return all incomplete offsets of buffered work in this shard, even if higher than the highest succeeded
     */
    public Set<Long> getAllIncompleteOffsets() {
        return incompleteOffsets.parallelStream()
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * @return incomplete offsets which are lower than the highest succeeded
     */
    // todo move down
    public Set<Long> getIncompleteOffsetsBelowHighestSucceeded() {
        long highestSucceeded = getOffsetHighestSucceeded();
        return incompleteOffsets.parallelStream()
                // todo less than or less than and equal?
                .filter(x -> x < highestSucceeded)
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * The highest seen offset for a partition.
     * <p>
     * Starts off as -1 - no data. Offsets in Kafka are never negative, so this is fine.
     */
    // visible for testing
    @Getter(PUBLIC)
    private long offsetHighestSeen;

    /**
     * Highest offset which has completed successfully ("succeeded").
     */
    @Getter(PUBLIC)
    private long offsetHighestSucceeded = -1L;

    /**
     * Highest continuous succeeded offset
     */
//    private long offsetHighestSequentialSucceeded = -1L;

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

    public PartitionState(TopicPartition tp, OffsetMapCodecManager.HighestOffsetAndIncompletes offsetData) {
        this.tp = tp;
        this.offsetHighestSeen = offsetData.getHighestSeenOffset().orElse(-1L);
        this.incompleteOffsets = new TreeSet<>(offsetData.getIncompleteOffsets());
        this.offsetHighestSucceeded = this.offsetHighestSeen;
    }

    public void maybeRaiseHighestSeenOffset(final long offset) {
        // rise the highest seen offset
        if (offset >= offsetHighestSeen) {
            log.trace("Updating highest seen - was: {} now: {}", offsetHighestSeen, offset);
            offsetHighestSeen = offset;
        }
    }

    /**
     * Removes all offsets that fall below the new low water mark.
     */
    public void truncateOffsets(final long nextExpectedOffset) {
        incompleteOffsets.removeIf(offset -> offset < nextExpectedOffset);
    }

    public void onOffsetCommitSuccess(final OffsetPair committed) {
        long nextExpectedOffset = committed.getSync().offset();
        truncateOffsets(nextExpectedOffset);

        // todo possibility this is run a different thread than controller?
        updateIncompletes(committed);
    }

    private void updateIncompletes(OffsetPair committed) {
        // todo - track dirty state here
    }


    boolean isDirty() {
        // todo return is dirty ?
        return true;
    }

    public boolean isRecordPreviouslyCompleted(final ConsumerRecord<K, V> rec) {
        long recOffset = rec.offset();
        // todo slow - maintain a hashset of offsets instead
        if (!getIncompleteOffsetsBelowHighestSucceeded().contains(recOffset)) {
            // if within the range of tracked offsets, must have been previously completed, as it's not in the incomplete set
            return recOffset <= offsetHighestSeen;
        } else {
            // we haven't recorded this far up, so must not have been processed yet
            return false;
        }
    }

    public boolean hasWorkInCommitQueue() {
        return !commitQueue.isEmpty();
    }

    public boolean hasWorkThatNeedsCommitting() {
        return commitQueue.values().parallelStream().anyMatch(WorkContainer::isUserFunctionSucceeded);
    }

    public int getCommitQueueSize() {
        return commitQueue.size();
    }

    public void onSuccess(WorkContainer<K, V> work) {
        long offset = work.offset();
        boolean removed = this.incompleteOffsets.remove(offset);
        assert (removed);

        updateHighestSucceededOffsetSoFar(work);
    }

    public void onFailure(WorkContainer<K, V> work) {
        // no-op
    }

    /**
     * Update highest Succeeded seen so far
     */
    private void updateHighestSucceededOffsetSoFar(WorkContainer<K, V> work) {
        long highestSucceeded = getOffsetHighestSucceeded();
        long thisOffset = work.offset();
        if (thisOffset > highestSucceeded) {
            log.trace("Updating highest completed - was: {} now: {}", highestSucceeded, thisOffset);
            this.offsetHighestSucceeded = thisOffset;
        }

//        updateHighestSucceededSequential(work);
    }

//    private void updateHighestSucceededSequential(WorkContainer<K, V> work) {
//        long offset = work.offset();
//        boolean thisOffsetIsNextAfterHighest = this.offsetHighestSequentialSucceeded + 1 == offset;
//        if (thisOffsetIsNextAfterHighest) {
//            // find the new highest sequentially succeeded
//            this.offsetHighestSequentialSucceeded = findHighestSucceeded();
//        }
//    }

    public void addWorkContainer(WorkContainer<K, V> wc) {
        maybeRaiseHighestSeenOffset(wc.offset());
        commitQueue.put(wc.offset(), wc);
        incompleteOffsets.add(wc.offset());
    }

    public void remove(Iterable<WorkContainer<?, ?>> workToRemove) {
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

    // todo maybe not needed in end
    @Value
    public static class OffsetPair {
        OffsetAndMetadata sync;
        Set<Long> incompletes;
    }

    // todo rename syncOffsetAndIncompleteEncodings
    public OffsetPair getCompletedEligibleOffsetsAndRemoveNew() {
        OffsetAndMetadata offsetMetadata = createOffsetAndMetadata();
        Set<Long> incompleteOffsets = getIncompleteOffsetsBelowHighestSucceeded();
        return new OffsetPair(offsetMetadata, incompleteOffsets);
    }

    private OffsetAndMetadata createOffsetAndMetadata() {
        long nextOffset = getNextExpectedPolledOffset();
        // try to encode
        Optional<String> payloadOpt = tryToEncodeOffsetsStartingAt(nextOffset);
        return payloadOpt
                .map(s -> new OffsetAndMetadata(nextOffset, s))
                .orElseGet(() -> new OffsetAndMetadata(nextOffset));
    }

    private long getNextExpectedPolledOffset() {
        return getOffsetHighestSequentialSucceeded() + 1;
    }

    public long getOffsetHighestSequentialSucceeded() {
        if (this.incompleteOffsets.isEmpty()) {
            return -1;
        } else {
            return this.incompleteOffsets.first() - 1;
        }
    }

    /**
     * Tries to encode the incomplete offsets for this partition. This may not be possible if there are none, or if no
     * encodings are possible ({@link NoEncodingPossibleException}. Encoding may not be possible of - see {@link
     * OffsetMapCodecManager#makeOffsetMetadataPayload}.
     *
     * @return if possible, the String encoded offset map
     */
    Optional<String> tryToEncodeOffsetsStartingAt(long offsetOfNextExpectedMessage) {
        if (incompleteOffsets.isEmpty()) {
            setAllowedMoreRecords(true);
            return empty();
        }

        try {
            // todo refactor use of null shouldn't be needed. Is OffsetMapCodecManager stateful? remove null #233
            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(null);
            String offsetMapPayload = om.makeOffsetMetadataPayload(offsetOfNextExpectedMessage, this);
            boolean mustStrip = updateBlockFromEncodingResult(offsetMapPayload);
            if (mustStrip) {
                return empty();
            } else {
                return of(offsetMapPayload);
            }
        } catch (NoEncodingPossibleException e) {
            setAllowedMoreRecords(false);
            log.warn("No encodings could be used to encode the offset map, skipping. Warning: messages might be replayed on rebalance.", e);
            return empty();
        }
    }

    /**
     * @return true if the payload is too large and must be stripped
     */
    private boolean updateBlockFromEncodingResult(String offsetMapPayload) {
        int metaPayloadLength = offsetMapPayload.length();
        boolean mustStrip = false;

        if (metaPayloadLength > DefaultMaxMetadataSize) {
            // exceeded maximum API allowed, strip the payload
            mustStrip = true;
            setAllowedMoreRecords(false);
            log.warn("Offset map data too large (size: {}) to fit in metadata payload hard limit of {} - cannot include in commit. " +
                            "Warning: messages might be replayed on rebalance. " +
                            "See kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize = {} and issue #47.",
                    metaPayloadLength, DefaultMaxMetadataSize, DefaultMaxMetadataSize);
        } else if (metaPayloadLength > getPressureThresholdValue()) { // and thus metaPayloadLength <= DefaultMaxMetadataSize
            // try to turn on back pressure before max size is reached
            setAllowedMoreRecords(false);
            log.warn("Payload size {} higher than threshold {}, but still lower than max {}. Will write payload, but will " +
                            "not allow further messages, in order to allow the offset data to shrink (via succeeding messages).",
                    metaPayloadLength, getPressureThresholdValue(), DefaultMaxMetadataSize);

        } else { // and thus (metaPayloadLength <= pressureThresholdValue)
            setAllowedMoreRecords(true);
            log.debug("Payload size {} within threshold {}", metaPayloadLength, getPressureThresholdValue());
        }

        return mustStrip;
    }

    private double getPressureThresholdValue() {
        return DefaultMaxMetadataSize * PartitionMonitor.getUSED_PAYLOAD_THRESHOLD_MULTIPLIER();
    }

}
