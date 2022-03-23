package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.offsets.NoEncodingPossibleException;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.Getter;
import lombok.NonNull;
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
import static lombok.AccessLevel.*;

@Slf4j
public class PartitionState<K, V> {

    @Getter
    private final TopicPartition tp;

    /**
     * A subset of Offsets, beyond the highest committable offset, which haven't been totally completed.
     */
    // todo why concurrent - doesn't need it?
    private final Set<WorkContainer<?, ?>> incompleteWorkContainers = new HashSet<>();

    /**
     * @return all incomplete offsets of buffered work in this shard, even if higher than the highest succeeded
     */
    public Set<Long> getAllIncompleteOffsets() {
        return incompleteWorkContainers.parallelStream()
                .map(WorkContainer::offset)
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * @return incomplete offsets which are lower than the highest succeeded
     */
    public Set<Long> getIncompleteOffsetsBelowHighestSucceeded() {
        long highestSucceeded = getOffsetHighestSucceeded();
        return incompleteWorkContainers.parallelStream()
                .map(WorkContainer::offset)
                .filter(x -> x < highestSucceeded)
                .collect(Collectors.toUnmodifiableSet());
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
     * Highest continuous succeeded offset
     */
    @Getter(PUBLIC)
    @Setter(PRIVATE)
    private long offsetHighestSequentialSucceeded = -1L;

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
        incompleteWorkContainers.removeIf(offset -> offset.offset() < nextExpectedOffset);
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
        boolean previouslyProcessed;
        long offset = rec.offset();
        if (incompleteWorkContainers.contains(offset)) {
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
        this.incompleteWorkContainers.remove(work);
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
            setOffsetHighestSucceeded(thisOffset);
        }
    }

    public void addWorkContainer(WorkContainer<K, V> wc) {
        maybeRaiseHighestSeenOffset(wc.offset());
        commitQueue.put(wc.offset(), wc);
        incompleteWorkContainers.add(wc);
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

    /**
     * Tries to encode the incomplete offsets for this partition. This may not be possible if there are none, or if no
     * encodings are possible ({@link NoEncodingPossibleException}. Encoding may not be possible of - see {@link
     * OffsetMapCodecManager#makeOffsetMetadataPayload}.
     *
     * @return if possible, the String encoded offset map
     */
    Optional<String> tryToEncodeOffsetsStartingAt(long offsetOfNextExpectedMessage) {
        if (incompleteWorkContainers.isEmpty()) {
            setAllowedMoreRecords(true);
            return Optional.empty();
        }

        try {
            // todo refactor use of null shouldn't be needed. Is OffsetMapCodecManager stateful? remove null #233
            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(null);
            String offsetMapPayload = om.makeOffsetMetadataPayload(offsetOfNextExpectedMessage, this);
            updateBlockFromEncodingResult(offsetMapPayload);
            return Optional.of(offsetMapPayload);
        } catch (NoEncodingPossibleException e) {
            setAllowedMoreRecords(false);
            log.warn("No encodings could be used to encode the offset map, skipping. Warning: messages might be replayed on rebalance.", e);
            return Optional.empty();
        }
    }

    private void updateBlockFromEncodingResult(String offsetMapPayload) {
        int metaPayloadLength = offsetMapPayload.length();

        if (metaPayloadLength > DefaultMaxMetadataSize) {
            // exceeded maximum API allowed, strip the payload
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
    }

    private double getPressureThresholdValue() {
        return DefaultMaxMetadataSize * PartitionMonitor.getUSED_PAYLOAD_THRESHOLD_MULTIPLIER();
    }

}
