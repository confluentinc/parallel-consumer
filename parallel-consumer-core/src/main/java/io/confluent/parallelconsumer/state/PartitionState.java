package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.BrokerPollSystem;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.offsets.NoEncodingPossibleException;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
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

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static lombok.AccessLevel.*;

/**
 * @see PartitionStateManager
 */
@ToString
@Slf4j
public class PartitionState<K, V> {

    private final PCModule<K, V> module;

    /**
     * Symbolic value for a parameter which is initialised as having an offset absent (instead of using Optional or
     * null)
     */
    public static final long KAFKA_OFFSET_ABSENCE = -1L;

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
     * <p>
     * Note that this may in some conditions, there may be a gap between this and the next offset to poll - that being,
     * there may be some number of transaction marker records above it, and the next offset to poll.
     */
    @Getter(PUBLIC)
    private long offsetHighestSucceeded = KAFKA_OFFSET_ABSENCE;

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
     * @see OffsetMapCodecManager#KAFKA_MAX_METADATA_SIZE_DEFAULT
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
    @ToString.Exclude
    private final NavigableMap<Long, WorkContainer<K, V>> commitQueue = new ConcurrentSkipListMap<>();

    private NavigableMap<Long, WorkContainer<K, V>> getCommitQueue() {
        return Collections.unmodifiableNavigableMap(commitQueue);
    }

    public PartitionState(PCModule<K, V> module, TopicPartition tp, OffsetMapCodecManager.HighestOffsetAndIncompletes offsetData) {
        this.module = module;
        this.tp = tp;
        this.offsetHighestSeen = offsetData.getHighestSeenOffset().orElse(KAFKA_OFFSET_ABSENCE);
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

    public void onOffsetCommitSuccess(OffsetAndMetadata committed) { //NOSONAR
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

        WorkContainer<K, V> removedFromQueue = this.commitQueue.remove(offset);
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
        long newOffset = wc.offset();
        maybeRaiseHighestSeenOffset(newOffset);
        commitQueue.put(newOffset, wc);
        incompleteOffsets.add(newOffset);
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

    /**
     * Defines as the offset one below the highest sequentially succeeded offset
     */
    private long getNextExpectedPolledOffset() {
        return getOffsetHighestSequentialSucceeded() + 1;
    }

    /**
     * @return all incomplete offsets of buffered work in this shard, even if higher than the highest succeeded
     */
    public Set<Long> getAllIncompleteOffsets() {
        //noinspection FuseStreamOperations - only in java 10
        return Collections.unmodifiableSet(incompleteOffsets.parallelStream()
                .collect(Collectors.toSet()));
    }

    /**
     * @return incomplete offsets which are lower than the highest succeeded
     */
    public Set<Long> getIncompleteOffsetsBelowHighestSucceeded() {
        long highestSucceeded = getOffsetHighestSucceeded();
        //noinspection FuseStreamOperations Collectors.toUnmodifiableSet since v10
        return Collections.unmodifiableSet(incompleteOffsets.parallelStream()
                // todo less than or less than and equal?
                .filter(x -> x < highestSucceeded)
                .collect(Collectors.toSet()));
    }

    /**
     * Defined for our purpose (as only used in definition of what offset to poll for next), as the offset one below the
     * lowest incomplete offset.
     */
    public long getOffsetHighestSequentialSucceeded() {
        if (this.incompleteOffsets.isEmpty()) {
            return this.offsetHighestSeen;
        } else {
            return this.incompleteOffsets.first() - 1;
        }
    }

    /**
     * Tries to encode the incomplete offsets for this partition. This may not be possible if there are none, or if no
     * encodings are possible ({@link NoEncodingPossibleException}. Encoding may not be possible of - see
     * {@link OffsetMapCodecManager#makeOffsetMetadataPayload}.
     *
     * @return if possible, the String encoded offset map
     */
    private Optional<String> tryToEncodeOffsets() {
        if (incompleteOffsets.isEmpty()) {
            setAllowedMoreRecords(true);
            return empty();
        }

        try {
            // todo refactor is OffsetMapCodecManager stateful? #233
            long offsetOfNextExpectedMessage = getNextExpectedPolledOffset();
            OffsetMapCodecManager<K, V> om = module.createOffsetMapCodecManager();
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

        if (metaPayloadLength > module.getMaxMetadataSize()) {
            // exceeded maximum API allowed, strip the payload
            mustStrip = true;
            setAllowedMoreRecords(false);
            log.warn("Offset map data too large (size: {}) to fit in metadata payload hard limit of {} - cannot include in commit. " +
                            "Warning: messages might be replayed on rebalance. " +
                            "See kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize = {} and issue #47.",
                    metaPayloadLength, module.getMaxMetadataSize(), OffsetMapCodecManager.KAFKA_MAX_METADATA_SIZE_DEFAULT);
        } else if (metaPayloadLength > getPressureThresholdValue()) { // and thus metaPayloadLength <= DefaultMaxMetadataSize
            // try to turn on back pressure before max size is reached
            setAllowedMoreRecords(false);
            log.warn("Payload size {} higher than threshold {}, but still lower than max {}. Will write payload, but will " +
                            "not allow further messages, in order to allow the offset data to shrink (via succeeding messages).",
                    metaPayloadLength,
                    getPressureThresholdValue(),
                    module.getMaxMetadataSize());

        } else { // and thus (metaPayloadLength <= pressureThresholdValue)
            setAllowedMoreRecords(true);
            log.debug("Payload size {} within threshold {}", metaPayloadLength, getPressureThresholdValue());
        }

        return mustStrip;
    }

    private double getPressureThresholdValue() {
        return module.getMaxMetadataSize() * module.getPayloadThresholdMultiplier();
    }

    public void onPartitionsRemoved(ShardManager<K, V> sm) {
        sm.removeAnyShardsReferencedBy(getCommitQueue());
    }

    /**
     * Convenience method for readability
     *
     * @return true if {@link #isAllowedMoreRecords()} is false
     * @see #isAllowedMoreRecords()
     */
    public boolean isBlocked() {
        return !isAllowedMoreRecords();
    }
}

