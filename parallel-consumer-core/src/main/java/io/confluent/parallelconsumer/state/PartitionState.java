package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.BrokerPollSystem;
import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.offsets.NoEncodingPossibleException;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.DefaultMaxMetadataSize;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static lombok.AccessLevel.*;

/**
 * Our view of our state of the partitions that we've been assigned.
 *
 * @see PartitionStateManager
 */
// todo class becoming large - possible to extract some functionality?
@ToString
@Slf4j
public class PartitionState<K, V> {

    /**
     * Symbolic value for a parameter which is initialised as having an offset absent (instead of using Optional or
     * null)
     */
    public static final long KAFKA_OFFSET_ABSENCE = -1L;

    private PCModule<K, V> module;

    /**
     * Used for adding work to, if it's been successfully added to our tracked state
     *
     * @see #maybeRegisterNewPollBatchAsWork
     */
    @NonNull
    private final ShardManager<K, V> sm;

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
    private ConcurrentSkipListSet<Long> incompleteOffsets;

    /**
     * Marker for the first record to be tracked. Used for some initial analysis.
     */
    private boolean noWorkAddedYet = true;

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
     * AKA high watermark (which is a deprecated description).
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
     */
    // todo rename - it's not a queue of things to be committed - it's a collection of incomplete offsets and their WorkContainers
    // todo delete? seems this can be replaced by #incompletes - the work container info isn't used
    @ToString.Exclude
    private final NavigableMap<Long, WorkContainer<K, V>> commitQueue = new ConcurrentSkipListMap<>();

    private NavigableMap<Long, WorkContainer<K, V>> getCommitQueue() {
        return Collections.unmodifiableNavigableMap(commitQueue);
    }

    public PartitionState(TopicPartition tp, OffsetMapCodecManager.HighestOffsetAndIncompletes offsetData) {
        this.tp = tp;
        this.offsetHighestSeen = offsetData.getHighestSeenOffset().orElse(KAFKA_OFFSET_ABSENCE);
        this.incompleteOffsets = new ConcurrentSkipListSet<>(offsetData.getIncompleteOffsets());
        this.offsetHighestSucceeded = this.offsetHighestSeen;
        this.sm = null;
        this.module = pcModule;
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

    // todo rename isRecordComplete()
    // todo add support for this to TruthGen
    public boolean isRecordPreviouslyCompleted(final ConsumerRecord<K, V> rec) {
        long recOffset = rec.offset();
        if (incompleteOffsets.contains(recOffset)) {
            // we haven't recorded this far up, so must not have been processed yet
            return false;
        } else {
            // if within the range of tracked offsets, must have been previously completed, as it's not in the incomplete set
            return recOffset <= offsetHighestSeen;
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

    private boolean epochIsStale(EpochAndRecordsMap<K, V>.RecordsAndEpoch recordsAndEpoch) {
        // do epochs still match? do a proactive check, but the epoch will be checked again at work completion as well
        var currentPartitionEpoch = getPartitionsAssignmentEpoch();
        Long epochOfInboundRecords = recordsAndEpoch.getEpochOfPartitionAtPoll();

        return !Objects.equals(epochOfInboundRecords, currentPartitionEpoch);
    }

    public void maybeRegisterNewPollBatchAsWork(@NonNull EpochAndRecordsMap<K, V>.RecordsAndEpoch recordsAndEpoch) {
        if (epochIsStale(recordsAndEpoch)) {
            log.debug("Inbound record of work has epoch ({}) not matching currently assigned epoch for the applicable partition ({}), skipping",
                    recordsAndEpoch.getEpochOfPartitionAtPoll(), getPartitionsAssignmentEpoch());
            return;
        }

        //
        long epochOfInboundRecords = recordsAndEpoch.getEpochOfPartitionAtPoll();
        List<ConsumerRecord<K, V>> recordPollBatch = recordsAndEpoch.getRecords();
        for (var aRecord : recordPollBatch) {
            if (isRecordPreviouslyCompleted(aRecord)) {
                log.trace("Record previously completed, skipping. offset: {}", aRecord.offset());
            } else {
                //noinspection ObjectAllocationInLoop
                var work = new WorkContainer<>(epochOfInboundRecords, aRecord, module);

                sm.addWorkContainer(work);
                addNewIncompleteWorkContainer(work);
            }
        }

    }

    public boolean isPartitionRemovedOrNeverAssigned() {
        return false;
    }

    public void addNewIncompleteWorkContainer(WorkContainer<K, V> wc) {
        long newOffset = wc.offset();

        maybeRaiseHighestSeenOffset(newOffset);
        commitQueue.put(newOffset, wc);

        // idempotently add the offset to our incompletes track - if it was already there from loading our metadata on startup, there is no affect
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
        if (isDirty()) return of(createOffsetAndMetadata());
        else return empty();
    }

    // visible for testing
    protected OffsetAndMetadata createOffsetAndMetadata() {
        Optional<String> payloadOpt = tryToEncodeOffsets();
        long nextOffset = getNextExpectedInitialPolledOffset();
        return payloadOpt.map(s -> new OffsetAndMetadata(nextOffset, s)).orElseGet(() -> new OffsetAndMetadata(nextOffset));
    }

    /**
     * Next offset expected to be polled, upon freshly connecting to a broker.
     * <p>
     * Defines as the offset one below the highest sequentially succeeded offset.
     */
    // visible for testing
    protected long getNextExpectedInitialPolledOffset() {
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
    public Set<Long> getIncompleteOffsetsBelowHighestSucceeded() {
        long highestSucceeded = getOffsetHighestSucceeded();
        //noinspection FuseStreamOperations Collectors.toUnmodifiableSet since v10
        return Collections.unmodifiableSet(incompleteOffsets.parallelStream()
                // todo less than or less than and equal?
                .filter(x -> x < highestSucceeded).collect(Collectors.toSet()));
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
            log.warn("Offset map data too large (size: {}) to fit in metadata payload hard limit of {} - cannot include in commit. " + "Warning: messages might be replayed on rebalance. " + "See kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize = {} and issue #47.", metaPayloadLength, DefaultMaxMetadataSize, DefaultMaxMetadataSize);
        } else if (metaPayloadLength > getPressureThresholdValue()) { // and thus metaPayloadLength <= DefaultMaxMetadataSize
            // try to turn on back pressure before max size is reached
            setAllowedMoreRecords(false);
            log.warn("Payload size {} higher than threshold {}, but still lower than max {}. Will write payload, but will " + "not allow further messages, in order to allow the offset data to shrink (via succeeding messages).", metaPayloadLength, getPressureThresholdValue(), DefaultMaxMetadataSize);

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
     * If the record is below the highest succeeded offset, then it is or will be represented in the current offset
     * encoding.
     * <p>
     * This may in fact be THE message holding up the partition - so must be retried.
     * <p>
     * In which case - don't want to skip it.
     * <p>
     * Generally speaking, completing more offsets below the highest succeeded (and thus the set represented in the
     * encoded payload), should usually reduce the payload size requirements.
     */
    private boolean isBlockingProgress(WorkContainer<?, ?> workContainer) {
        return workContainer.offset() < getOffsetHighestSucceeded();
    }

    /**
     * TODO docs
     */
    public boolean couldBeTakenAsWork(WorkContainer<K, V> workContainer) {
        if (checkIfWorkIsStale(workContainer)) {
            log.debug("Work is in queue with stale epoch or no longer assigned. Skipping. Shard it came from will/was removed during partition revocation. WC: {}", workContainer);
            return false;
        } else if (isAllowedMoreRecords()) {
            return true;
        } else if (isBlockingProgress(workContainer)) {
            // allow record to be taken, even if partition is blocked, as this record completion may reduce payload size requirement
            return true;
        } else {
            log.debug("Not allowed more records for the partition ({}) as set from previous encode run (blocked), that this " +
                            "record ({}) belongs to, due to offset encoding back pressure, is within the encoded payload already (offset lower than highest succeeded, " +
                            "not in flight ({}), continuing on to next container in shardEntry.",
                    workContainer.getTopicPartition(), workContainer.offset(), workContainer.isNotInFlight());
            return false;
        }
    }

    /**
     * Have our partitions been revoked?
     * <p>
     * This state is rare, as shards or work get removed upon partition revocation, although under busy load it might
     * occur we don't synchronize over PartitionState here so it's a bit racey, but is handled and eventually settles.
     *
     * @return true if epoch doesn't match, false if ok
     */
    boolean checkIfWorkIsStale(final WorkContainer<?, ?> workContainer) {
        Long currentPartitionEpoch = getPartitionsAssignmentEpoch();
        long workEpoch = workContainer.getEpoch();

        boolean partitionNotAssigned = isPartitionRemovedOrNeverAssigned();

        boolean epochMissMatch = currentPartitionEpoch != workEpoch;

        if (epochMissMatch || partitionNotAssigned) {
            log.debug("Epoch mismatch {} vs {} for record {}. Skipping message - it's partition has already assigned to a different consumer.",
                    workEpoch, currentPartitionEpoch, workContainer);
            return true;
        }
        return false;
    }

    /**
     * todo docs
     */
    private Long getPartitionsAssignmentEpoch() {
        // todo teach PartitionState to know it's Epoch, move this into PartitionState
        throw new RuntimeException();
    }

}

