package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.BrokerPollSystem;
import io.confluent.parallelconsumer.offsets.NoEncodingPossibleException;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.DefaultMaxMetadataSize;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static lombok.AccessLevel.*;

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
     * <p>
     * Needs to be concurrent because, the committer requesting the data to commit may be another thread - the broker
     * polling sub system - {@link BrokerPollSystem#maybeDoCommit}. The alternative to having this as a concurrent
     * collection, would be to have the control thread prepare possible commit data on every cycle, and park that data
     * so that the broker polling thread can grab it, if it wants to commit - i.e. the poller would not prepare/query
     * the data for itself. See also #200 Refactor: Consider a shared nothing architecture.
     */
    private final ConcurrentSkipListSet<Long> incompleteOffsets;

    /**
     * Cache view of the state of the partition. Is set dirty when the incomplete state of any offset changes. Is set
     * clean after a successful commit of the state.
     */
    @Setter(PRIVATE)
    @Getter(PRIVATE)
    private boolean dirty;

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
    @Setter(PRIVATE)
    private boolean allowedMoreRecords = true;

    /**
     * Map of offsets to WorkUnits.
     * <p>
     * Need to record globally consumed records, to ensure correct offset order committal. Cannot rely on incrementally
     * advancing offsets, as this isn't a guarantee of kafka's.
     * <p>
     * Concurrent because either the broker poller thread or the control thread may be requesting offset to commit
     * ({@link #getCommitDataIfDirty()}), or reading upon {@link #onPartitionsRemoved}
     */
    // todo doesn't need to be concurrent any more?
    private final NavigableMap<Long, WorkContainer<K, V>> commitQueue = new ConcurrentSkipListMap<>();

    private NavigableMap<Long, WorkContainer<K, V>> getCommitQueue() {
        return Collections.unmodifiableNavigableMap(commitQueue);
    }

    public PartitionState(TopicPartition tp, OffsetMapCodecManager.HighestOffsetAndIncompletes offsetData) {
        this.tp = tp;
        this.offsetHighestSeen = offsetData.getHighestSeenOffset().orElse(-1L);
        this.incompleteOffsets = new ConcurrentSkipListSet<>(offsetData.getIncompleteOffsets());
        this.offsetHighestSucceeded = this.offsetHighestSeen;
    }

    private void maybeRaiseHighestSeenOffset(final long offset) {
        // rise the highest seen offset
        if (offset >= offsetHighestSeen) {
            log.trace("Updating highest seen - was: {} now: {}", offsetHighestSeen, offset);
            offsetHighestSeen = offset;
        }
    }

    public void onOffsetCommitSuccess(final OffsetAndMetadata committed) {
        setClean();
    }

    private void setClean() {
        setDirty(false);
    }

    private void setDirty() {
        setDirty(true);
    }

    public boolean isRecordPreviouslyCompleted(final ConsumerRecord<K, V> rec) {
        long recOffset = rec.offset();
        if (!incompleteOffsets.contains(recOffset)) {
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

    public int getCommitQueueSize() {
        return commitQueue.size();
    }

    public void onSuccess(WorkContainer<K, V> work) {
        long offset = work.offset();
        WorkContainer<K, V> removedFromQueue = this.commitQueue.remove(work.offset());
        assert (removedFromQueue != null);
        boolean removedFromIncompletes = this.incompleteOffsets.remove(offset);
        assert (removedFromIncompletes);

        updateHighestSucceededOffsetSoFar(work);

        setDirty();
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
    }

    public void addWorkContainer(WorkContainer<K, V> wc) {
        maybeRaiseHighestSeenOffset(wc.offset());
        commitQueue.put(wc.offset(), wc);
        incompleteOffsets.add(wc.offset());
    }

    /**
     * Has this partition been removed? No.
     *
     * @return by definition false in this implementation
     */
    public boolean isRemoved() {
        return false;
    }

    public Optional<OffsetAndMetadata> getCommitDataIfDirty() {
        if (isDirty())
            return of(createOffsetAndMetadata());
        else
            return empty();
    }

    private OffsetAndMetadata createOffsetAndMetadata() {
        Optional<String> payloadOpt = tryToEncodeOffsets();
        long nextOffset = getNextExpectedPolledOffset();
        return payloadOpt
                .map(s -> new OffsetAndMetadata(nextOffset, s))
                .orElseGet(() -> new OffsetAndMetadata(nextOffset));
    }

    private long getNextExpectedPolledOffset() {
        return getOffsetHighestSequentialSucceeded() + 1;
    }

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
    public Set<Long> getIncompleteOffsetsBelowHighestSucceeded() {
        long highestSucceeded = getOffsetHighestSucceeded();
        return incompleteOffsets.parallelStream()
                // todo less than or less than and equal?
                .filter(x -> x < highestSucceeded)
                .collect(Collectors.toUnmodifiableSet());
    }

    public long getOffsetHighestSequentialSucceeded() {
        if (this.incompleteOffsets.isEmpty()) {
            return this.offsetHighestSeen;
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
    private Optional<String> tryToEncodeOffsets() {
        if (incompleteOffsets.isEmpty()) {
            setAllowedMoreRecords(true);
            return empty();
        }

        try {
            // todo refactor use of null shouldn't be needed. Is OffsetMapCodecManager stateful? remove null #233
            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(null);
            long offsetOfNextExpectedMessage = getNextExpectedPolledOffset();
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

    public void onPartitionsRemoved(ShardManager<K, V> sm) {
        sm.removeAnyShardsReferencedBy(getCommitQueue());
    }
}
