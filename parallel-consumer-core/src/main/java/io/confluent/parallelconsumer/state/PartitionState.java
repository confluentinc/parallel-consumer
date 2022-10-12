package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.BrokerPollSystem;
import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;
import io.confluent.parallelconsumer.offsets.NoEncodingPossibleException;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniSets;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.JavaUtils.getFirst;
import static io.confluent.csid.utils.JavaUtils.getLast;
import static io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.DefaultMaxMetadataSize;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static lombok.AccessLevel.*;

/**
 * Our view of the state of the partitions that we've been assigned.
 *
 * @see PartitionStateManager
 */
@ToString
@Slf4j
public class PartitionState<K, V> {

    /**
     * Symbolic value for a parameter which is initialised as having an offset absent (instead of using Optional or
     * null)
     */
    public static final long KAFKA_OFFSET_ABSENCE = -1L;

    @Getter
    private final TopicPartition tp;

    /**
     * Offsets beyond the highest committable offset (see {@link #getOffsetHighestSequentialSucceeded()}) which haven't
     * totally succeeded. Based on decoded metadata and polled records (not offset ranges).
     * <p>
     * <p>
     * <h2>How does this handle gaps in the offsets in the source partitions?:</h2>
     * <p>
     * We track per record acknowledgement, by only storing the offsets of records <em>OF WHICH WE'VE RECEIVED</em>
     * through {@link KafkaConsumer#poll} calls.
     * <p>
     * This is as explicitly opposed to looking at the lowest offset we've polled, and synthetically creating a list of
     * EXPECTED offsets from the range from it to the highest polled. If we were to construct this offset range
     * synthetically like this, then we would need to expect to process/receive records which might not exist, for
     * whatever reason, usually due to compaction.
     * <p>
     * Instead, the offsets tracked are only determined from the records we've given to process from the broker - we
     * make no assumptions about which offsets exist. This way we don't have to worry about gaps in the offsets. Also, a
     * nice outcome of this is that a gap in the offsets is effectively the same as, as far as we're concerned, an
     * offset which has succeeded - because either way we have no action to take.
     * <p>
     * This is independent of the actual queued {@link WorkContainer}s. This is because to start with, data about
     * incomplete offsets come from the encoded metadata payload that gets committed along with the highest committable
     * offset ({@link #getOffsetHighestSequentialSucceeded()}) and so we don't yet have ConsumerRecord's for those
     * offsets until we start polling for them. And so they are not always in sync.
     * <p>
     * <p>
     * <h2>Concurrency:</h2>
     * <p>
     * Needs to be concurrent because, the committer requesting the data to commit may be another thread - the broker
     * polling sub system - {@link BrokerPollSystem#maybeDoCommit}. The alternative to having this as a concurrent
     * collection, would be to have the control thread prepare possible commit data on every cycle, and park that data
     * so that the broker polling thread can grab it, if it wants to commit - i.e. the poller would not prepare/query
     * the data for itself. This requirement is removed in the upcoming PR #200 Refactor: Consider a shared nothing
     * architecture.
     *
     * @see io.confluent.parallelconsumer.offsets.BitSetEncoder for disucssion on how this is impacts per record ack
     *         storage
     */
    private NavigableSet<Long> incompleteOffsets;

    /**
     * Marks whether any {@link WorkContainer}s have been added yet or not. Used for some initial poll analysis.
     */
    private boolean bootstrapPhase = true;

    /**
     * Cache view of the state of the partition. Is set dirty when the incomplete state of any offset changes. Is set
     * clean after a successful commit of the state.
     */
    @Setter(PRIVATE)
    @Getter(PACKAGE)
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
     * <p>
     * Note that as we only encode our offset map up to the highest succeeded offset (as encoding higher has no value),
     * upon bootstrap, this will always start off as the same as the {@link #offsetHighestSeen}.
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
     * @see OffsetMapCodecManager#DefaultMaxMetadataSize
     */
    @Getter(PACKAGE)
    @Setter(PRIVATE)
    private boolean allowedMoreRecords = true;

    /**
     * Map of offsets to {@link WorkContainer}s.
     * <p>
     * Need to record globally consumed records, to ensure correct offset order committal. Cannot rely on incrementally
     * advancing offsets, as this isn't a guarantee of kafka's (see {@link #incompleteOffsets}).
     * <p>
     * Concurrent because either the broker poller thread or the control thread may be requesting offset to commit
     * ({@link #getCommitDataIfDirty()}), or reading upon {@link #onPartitionsRemoved}. This requirement is removed in
     * the upcoming PR #200 Refactor: Consider a shared nothing * architecture.
     *
     * @deprecated the map structure isn't used anymore and can be replaced with the offsets tracked in
     *         {@link #incompleteOffsets} - refactored future PR
     */
    @ToString.Exclude
    @Deprecated
    private NavigableMap<Long, WorkContainer<K, V>> commitQueue = new ConcurrentSkipListMap<>();

    private NavigableMap<Long, WorkContainer<K, V>> getCommitQueue() {
        return Collections.unmodifiableNavigableMap(commitQueue);
    }

    public PartitionState(TopicPartition tp, OffsetMapCodecManager.HighestOffsetAndIncompletes offsetData) {
        this.tp = tp;

        initStateFromOffsetData(offsetData);
    }

    private void initStateFromOffsetData(OffsetMapCodecManager.HighestOffsetAndIncompletes offsetData) {
        this.offsetHighestSeen = offsetData.getHighestSeenOffset().orElse(KAFKA_OFFSET_ABSENCE);
        this.incompleteOffsets = new ConcurrentSkipListSet<>(offsetData.getIncompleteOffsets());
        this.offsetHighestSucceeded = this.offsetHighestSeen; // by definition, as we only encode up to the highest seen offset (inclusive)
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

    public void addNewIncompleteWorkContainer(WorkContainer<K, V> wc) {
        long newOffset = wc.offset();

        maybeRaiseHighestSeenOffset(newOffset);
        commitQueue.put(newOffset, wc);

        // idempotently add the offset to our incompletes track - if it was already there from loading our metadata on startup, there is no affect
        incompleteOffsets.add(newOffset);
    }

    /**
     * If the offset is higher than expected, according to the previously committed / polled offset, truncate up to it.
     * Offsets between have disappeared and will never be polled again.
     * <p>
     * Only runs if this is the first {@link WorkContainer} to be added since instantiation.
     */
    private void maybeTruncateBelowOrAbove(long polledOffset) {
        if (bootstrapPhase) {
            bootstrapPhase = false;
        } else {
            // Not bootstrap phase anymore, so not checking for truncation
            return;
        }

        long expectedBootstrapRecordOffset = getNextExpectedInitialPolledOffset();

        boolean pollAboveExpected = polledOffset > expectedBootstrapRecordOffset;

        boolean pollBelowExpected = polledOffset < expectedBootstrapRecordOffset;

        if (pollAboveExpected) {
            // previously committed offset record has been removed, or manual reset to higher offset detected
            log.warn("Truncating state - removing records lower than {}. Offsets have been removed from the partition by the broker or committed offset has been raised. Bootstrap polled {} but " +
                            "expected {} from loaded commit data. Could be caused by record retention or compaction.",
                    polledOffset,
                    polledOffset,
                    expectedBootstrapRecordOffset);

            // truncate
            this.incompleteOffsets = incompleteOffsets.tailSet(polledOffset, true);
            this.commitQueue = commitQueue.tailMap(polledOffset, true);
        } else if (pollBelowExpected) {
            // manual reset to lower offset detected
            log.warn("Bootstrap polled offset has been reset to an earlier offset ({}) - truncating state - all records " +
                            "above (including this) will be replayed. Was expecting {} but bootstrap poll was {}.",
                    polledOffset,
                    expectedBootstrapRecordOffset,
                    polledOffset
            );

            // reset
            var resetHighestSeenOffset = Optional.<Long>empty();
            var resetIncompletesMap = UniSets.<Long>of();
            var offsetData = new OffsetMapCodecManager.HighestOffsetAndIncompletes(resetHighestSeenOffset, resetIncompletesMap);
            initStateFromOffsetData(offsetData);
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

    public Optional<OffsetAndMetadata> getCommitDataIfDirty() {
        return isDirty() ?
                of(createOffsetAndMetadata()) :
                empty();
    }

    // visible for testing
    protected OffsetAndMetadata createOffsetAndMetadata() {
        Optional<String> payloadOpt = tryToEncodeOffsets();
        long nextOffset = getNextExpectedInitialPolledOffset();
        return payloadOpt
                .map(encodedOffsets -> new OffsetAndMetadata(nextOffset, encodedOffsets))
                .orElseGet(() -> new OffsetAndMetadata(nextOffset));
    }

    /**
     * Next offset expected to be polled, upon freshly connecting to a broker.
     * <p>
     * Defines as the offset one below the highest sequentially succeeded offset.
     */
    // visible for testing
    // todo change back to protected? and enable protected level managed truth (seems to be limited to public)
    public long getNextExpectedInitialPolledOffset() {
        return getOffsetHighestSequentialSucceeded() + 1;
    }

    /**
     * @return all incomplete offsets of buffered work in this shard, even if higher than the highest succeeded
     */
    public Set<Long> getAllIncompleteOffsets() {
        //noinspection FuseStreamOperations - only in java 10
        return Collections.unmodifiableSet(incompleteOffsets.parallelStream().collect(Collectors.toSet()));
    }

    /**
     * @return incomplete offsets which are lower than the highest succeeded
     */
    // todo change from Set to List (order)
    public Set<Long> getIncompleteOffsetsBelowHighestSucceeded() {
        long highestSucceeded = getOffsetHighestSucceeded();
        //noinspection FuseStreamOperations Collectors.toUnmodifiableSet since v10
        return Collections.unmodifiableSet(incompleteOffsets.parallelStream()
                .filter(x -> x < highestSucceeded)
                .collect(Collectors.toSet()));
    }

    /**
     * The offset which is itself, and all before, all successfully completed (or skipped).
     * <p>
     * Defined for our purpose (as only used in definition of what offset to poll for next), as the offset one below the
     * lowest incomplete offset.
     */
    public long getOffsetHighestSequentialSucceeded() {
        /*
         * Capture the current value in case it's changed during this operation - because if more records are added to
         * the queue, after looking at the incompleteOffsets, offsetHighestSeen could increase drastically and will be
         * incorrect for the value of getOffsetHighestSequentialSucceeded. So this is a ~pessimistic solution - as in a
         * race case, there may be a higher getOffsetHighestSequentialSucceeded from the incompleteOffsets collection,
         * but it will always at lease be pessimistically correct in terms of committing offsets to the broker.
         *
         * See #200 for the complete correct solution.
         */
        long currentOffsetHighestSeen = offsetHighestSeen;
        Long firstIncompleteOffset = incompleteOffsets.ceiling(KAFKA_OFFSET_ABSENCE);
        boolean incompleteOffsetsWasEmpty = firstIncompleteOffset == null;

        if (incompleteOffsetsWasEmpty) {
            return currentOffsetHighestSeen;
        } else {
            return firstIncompleteOffset - 1;
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
            // todo refactor use of null shouldn't be needed. Is OffsetMapCodecManager stateful? remove null #233
            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(null);
            long offsetOfNextExpectedMessage = getNextExpectedInitialPolledOffset();
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
        return DefaultMaxMetadataSize * PartitionStateManager.getUSED_PAYLOAD_THRESHOLD_MULTIPLIER();
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

    /**
     * Each time we poll a patch of records, check to see that as expected our tracked incomplete offsets exist in the
     * set, otherwise they must have been removed from the underlying partition and should be removed from our tracking
     * as we'll ever be given the record again to retry.
     * <p>
     * <p>
     * Also, does {@link #maybeTruncateBelowOrAbove}.
     */
    @SuppressWarnings("OptionalGetWithoutIsPresent") // checked with isEmpty
    protected void maybeTruncateOrPruneTrackedOffsets(EpochAndRecordsMap<?, ?>.RecordsAndEpoch polledRecordBatch) {
        var records = polledRecordBatch.getRecords();

        if (records.isEmpty()) {
            log.warn("Polled an empty batch of records? {}", polledRecordBatch);
            return;
        }

        var low = getFirst(records).get().offset(); // NOSONAR see #isEmpty

        maybeTruncateBelowOrAbove(low);

        // build the hash set once, so we can do random access checks of our tracked incompletes
        var polledOffsetLookup = records.stream()
                .map(ConsumerRecord::offset)
                .collect(Collectors.toSet());

        var high = getLast(records).get().offset(); // NOSONAR see #isEmpty

        // for the incomplete offsets within this range of poll batch
        var incompletesWithinPolledBatch = incompleteOffsets.subSet(low, true, high, true);
        var offsetsToRemoveFromTracking = new ArrayList<Long>();
        for (long incompleteOffset : incompletesWithinPolledBatch) {
            boolean offsetMissingFromPolledRecords = !polledOffsetLookup.contains(incompleteOffset);

            if (offsetMissingFromPolledRecords) {
                offsetsToRemoveFromTracking.add(incompleteOffset);
                // don't need to remove it from the #commitQueue, as it would never have been added
            }
        }
        if (!offsetsToRemoveFromTracking.isEmpty()) {
            log.warn("Offsets {} have been removed from partition {} (as they were not been returned within a polled batch " +
                            "which should have contained them - batch offset range is {} to {}), so they be removed " +
                            "from tracking state, as they will never be sent again to be retried. " +
                            "This can be caused by PC rebalancing across a partition which has been compacted on offsets above the committed " +
                            "base offset, after initial load and before a rebalance.",
                    offsetsToRemoveFromTracking,
                    getTp(),
                    low,
                    high
            );
            boolean removedCheck = incompleteOffsets.removeAll(offsetsToRemoveFromTracking);
            assert removedCheck;
        }

    }

}

