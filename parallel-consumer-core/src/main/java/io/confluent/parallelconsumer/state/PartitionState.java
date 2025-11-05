package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2025 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.internal.BrokerPollSystem;
import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.metrics.PCMetrics;
import io.confluent.parallelconsumer.metrics.PCMetricsDef;
import io.confluent.parallelconsumer.offsets.NoEncodingPossibleException;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tag;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.JavaUtils.*;
import static io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.DefaultMaxMetadataSize;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static lombok.AccessLevel.*;

/**
 * Our view of the state of the partitions that we've been assigned.
 *
 * @author Antony Stubbs
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

    private final PCModule<K, V> module;

    @NonNull
    @Getter
    private final TopicPartition tp;

    /**
     * Offsets beyond the highest committable offset (see {@link #getOffsetHighestSequentialSucceeded()}) which haven't
     * totally succeeded. Based on decoded metadata and polled records (not offset ranges).
     * <p>
     * Mapped to the corresponding {@link ConsumerRecord}, once it's been polled from the broker.
     * <p>
     * Initially mapped to an empty optional, until the record is polled from the broker, because we initially get only
     * the incomplete offsets decoded from the metadata payload first, before receiving the records from poll requests.
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
    @NonNull
    @Setter(PACKAGE)
    private ConcurrentSkipListMap<Long, Optional<ConsumerRecord<K, V>>> incompleteOffsets;

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
     * AKA high watermark (which is a deprecated description).
     *
     * @see OffsetMapCodecManager#DefaultMaxMetadataSize
     */
    @Getter(PACKAGE)
    @Setter(PRIVATE)
    private boolean allowedMoreRecords = true;

    /**
     * The Epoch of the generation of partition assignment, for fencing off invalid work.
     * <p>
     * Will unified actor partition assignment messages, epochs may no longer be needed.
     */
    @Getter
    private final long partitionsAssignmentEpoch;

    private long lastCommittedOffset;
    private Gauge lastCommittedOffsetGauge;
    private Gauge highestSeenOffsetGauge;
    private Gauge highestCompletedOffsetGauge;
    private Gauge highestSequentialSucceededOffsetGauge;
    private Gauge numberOfIncompletesGauge;
    private Gauge ephochGauge;
    private DistributionSummary ratioPayloadUsedDistributionSummary;
    private DistributionSummary ratioMetadataSpaceUsedDistributionSummary;
    private final PCMetrics pcMetrics;
    private final OffsetMapCodecManager<K, V> om;

    /**
     * Additional flag to prevent overwriting dirty state that was updated during commit execution window - so that any
     * subsequent offsets completed while commit is being performed could mark state as dirty and retain the dirty state
     * on commit completion. In tight race condition - it may be set just before offset is completed and included in
     * commit data collection - so it is a little bit pessimistic - that may cause an additional unnecessary commit on
     * next commit cycle - but it is highly unlikely as throughput has to be high for this to occur - but with high
     * throughput there will be other offsets ready to commit anyway.
     */
    private boolean stateChangedSinceCommitStart = false;


    public PartitionState(long newEpoch,
                          PCModule<K, V> pcModule,
                          TopicPartition topicPartition,
                          OffsetMapCodecManager.HighestOffsetAndIncompletes offsetData) {
        this.module = pcModule;

        this.tp = topicPartition;
        this.partitionsAssignmentEpoch = newEpoch;
        this.pcMetrics = module.pcMetrics();
        initStateFromOffsetData(offsetData);
        initMetrics();
        this.om = new OffsetMapCodecManager<>(pcModule);
    }

    private void initStateFromOffsetData(OffsetMapCodecManager.HighestOffsetAndIncompletes offsetData) {
        this.offsetHighestSeen = offsetData.getHighestSeenOffset().orElse(KAFKA_OFFSET_ABSENCE);

        this.incompleteOffsets = new ConcurrentSkipListMap<>();
        offsetData.getIncompleteOffsets()
                .forEach(offset -> incompleteOffsets.put(offset, Optional.empty()));

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
        lastCommittedOffset = committed.offset();
        setClean();
    }

    private void setClean() {
        if (!stateChangedSinceCommitStart) {
            setDirty(false);
        }
    }

    private void setDirty() {
        stateChangedSinceCommitStart = true;
        setDirty(true);
    }

    // todo rename isRecordComplete()
    // todo add support for this to TruthGen
    public boolean isRecordPreviouslyCompleted(final ConsumerRecord<K, V> rec) {
        long recOffset = rec.offset();
        if (incompleteOffsets.containsKey(recOffset)) {
            // we haven't recorded this far up, so must not have been processed yet
            return false;
        } else {
            // if within the range of tracked offsets, must have been previously completed, as it's not in the incomplete set
            return recOffset <= offsetHighestSucceeded;
        }
    }

    public boolean hasIncompleteOffsets() {
        return !incompleteOffsets.isEmpty();
    }

    public int getNumberOfIncompleteOffsets() {
        return incompleteOffsets.size();
    }

    public void onSuccess(long offset) {
        //noinspection OptionalAssignedToNull - null check to see if key existed
        boolean removedFromIncompletes = this.incompleteOffsets.remove(offset) != null; // NOSONAR
        assert (removedFromIncompletes);

        updateHighestSucceededOffsetSoFar(offset);

        setDirty();
    }

    public void onFailure(WorkContainer<K, V> work) {
        // no-op
    }

    /**
     * Update highest Succeeded seen so far
     */
    private void updateHighestSucceededOffsetSoFar(long thisOffset) {
        long highestSucceeded = getOffsetHighestSucceeded();
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
        maybeTruncateOrPruneTrackedOffsets(recordsAndEpoch);

        //
        long epochOfInboundRecords = recordsAndEpoch.getEpochOfPartitionAtPoll();
        List<ConsumerRecord<K, V>> recordPollBatch = recordsAndEpoch.getRecords();
        for (var aRecord : recordPollBatch) {
            if (isRecordPreviouslyCompleted(aRecord)) {
                log.trace("Record previously completed, skipping. offset: {}", aRecord.offset());
            } else {
                getShardManager().addWorkContainer(epochOfInboundRecords, aRecord);
                addNewIncompleteRecord(aRecord);
            }
        }

    }

    /**
     * Used for adding work to, if it's been successfully added to our tracked state
     *
     * @see #maybeRegisterNewPollBatchAsWork
     */
    private ShardManager<K, V> getShardManager() {
        return module.workManager().getSm();
    }

    public boolean isPartitionRemovedOrNeverAssigned() {
        return false;
    }

    // visible for legacy testing
    public void addNewIncompleteRecord(ConsumerRecord<K, V> record) {
        long offset = record.offset();
        maybeRaiseHighestSeenOffset(offset);

        // idempotently add the offset to our incompletes track - if it was already there from loading our metadata on startup, there is no affect
        incompleteOffsets.put(offset, Optional.of(record));
    }


    /**
     * If the offset is higher than expected, according to the previously committed / polled offset, truncate up to it.
     * If lower, reset down to it.
     * <p>
     * Only runs if this is the first {@link ConsumerRecord} to be added since instantiation.
     * <p>
     * Can be caused by the offset reset policy of the underlying consumer.
     */
    private void maybeTruncateBelowOrAbove(long bootstrapPolledOffset) {
        if (bootstrapPhase) {
            bootstrapPhase = false;
        } else {
            // Not bootstrap phase anymore, so not checking for truncation
            return;
        }

        // during bootstrap, getOffsetToCommit() will return the offset of the last record committed, so we can use that to determine if we need to truncate
        long expectedBootstrapRecordOffset = getOffsetToCommit();

        boolean pollAboveExpected = bootstrapPolledOffset > expectedBootstrapRecordOffset;

        boolean pollBelowExpected = bootstrapPolledOffset < expectedBootstrapRecordOffset;

        if (pollAboveExpected) {
            // previously committed offset record has been removed from the topic, so we need to truncate up to it
            log.warn("Truncating state - removing records lower than {} from partition {} of topic {}. Offsets have been removed from the partition " +
                            "by the broker or committed offset has been raised. Bootstrap polled {} but expected {} from loaded commit data. " +
                            "Could be caused by record retention or compaction and offset reset policy LATEST.",
                    bootstrapPolledOffset,
                    this.tp.partition(),
                    this.tp.topic(),
                    bootstrapPolledOffset,
                    expectedBootstrapRecordOffset);

            // truncate
            final NavigableSet<Long> incompletesToPrune = incompleteOffsets.keySet().headSet(bootstrapPolledOffset, false);
            incompletesToPrune.forEach(incompleteOffsets::remove);
        } else if (pollBelowExpected) {
            // reset to lower offset detected, so we need to reset our state to match
            log.warn("Bootstrap polled offset has been reset to an earlier offset ({}) for partition {} of topic {} - truncating state - all records " +
                            "above (including this) will be replayed. Was expecting {} but bootstrap poll was {}. " +
                            "Could be caused by record retention or compaction and offset reset policy EARLIEST.",
                    bootstrapPolledOffset,
                    this.tp.partition(),
                    this.tp.topic(),
                    expectedBootstrapRecordOffset,
                    bootstrapPolledOffset
            );

            // reset
            var offsetData = OffsetMapCodecManager.HighestOffsetAndIncompletes.of();
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
        if (isDirty()) {
            // setting the flag so that any subsequent offset completed while commit is being performed could mark state as dirty
            // and retain the dirty state on commit completion.
            stateChangedSinceCommitStart = false;
            return of(createOffsetAndMetadata());
        }
        return empty();
    }

    // visible for testing
    protected OffsetAndMetadata createOffsetAndMetadata() {
        // use tuple to make sure getOffsetToCommit is invoked only once to avoid dirty read
        // and commit the wrong offset
        ParallelConsumer.Tuple<Optional<String>, Long> tuple = tryToEncodeOffsets();
        Optional<String> payloadOpt = tuple.getLeft();
        long nextOffset = tuple.getRight();
        return payloadOpt
                .map(encodedOffsets -> new OffsetAndMetadata(nextOffset, encodedOffsets))
                .orElseGet(() -> new OffsetAndMetadata(nextOffset));
    }

    /**
     * Next offset expected to be polled, upon freshly connecting to a broker.
     * <p>
     * Defined as the offset, one below the highest sequentially succeeded offset.
     */
    // visible for testing
    protected long getOffsetToCommit() {
        return getOffsetHighestSequentialSucceeded() + 1;
    }

    /**
     * @return all incomplete offsets of buffered work in this shard, even if higher than the highest succeeded
     */
    public List<Long> getAllIncompleteOffsets() {
        //noinspection FuseStreamOperations - only in java 10
        return Collections.unmodifiableList(incompleteOffsets.keySet().parallelStream().collect(Collectors.toList()));
    }

    /**
     * @return incomplete offsets which are lower than the highest succeeded
     */
    public SortedSet<Long> getIncompleteOffsetsBelowHighestSucceeded() {
        long highestSucceeded = getOffsetHighestSucceeded();
        return incompleteOffsets.keySet().parallelStream()
                .filter(x -> x < highestSucceeded)
                .collect(toTreeSet());
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
        // use offsetHighestSucceeded instead of offsetHighestSeen to fix issue #826
        long currentOffsetHighestSeen = offsetHighestSucceeded;
        Long firstIncompleteOffset = incompleteOffsets.keySet().ceiling(KAFKA_OFFSET_ABSENCE);
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
    private ParallelConsumer.Tuple<Optional<String>, Long> tryToEncodeOffsets() {
        long offsetOfNextExpectedMessage = getOffsetToCommit();

        if (incompleteOffsets.isEmpty()) {
            setAllowedMoreRecords(true);
            return ParallelConsumer.Tuple.pairOf(empty(), offsetOfNextExpectedMessage);
        }

        try {
            // todo refactor use of null shouldn't be needed. Is OffsetMapCodecManager stateful? remove null #233
            var offsetRange = getOffsetHighestSucceeded() - offsetOfNextExpectedMessage;
            String offsetMapPayload = om.makeOffsetMetadataPayload(offsetOfNextExpectedMessage, this);
            ratioPayloadUsedDistributionSummary.record(offsetMapPayload.length() / (double) offsetRange);
            ratioMetadataSpaceUsedDistributionSummary.record(offsetMapPayload.length() / (double) OffsetMapCodecManager.DefaultMaxMetadataSize);
            boolean mustStrip = updateBlockFromEncodingResult(offsetMapPayload);
            if (mustStrip) {
                return ParallelConsumer.Tuple.pairOf(empty(), offsetOfNextExpectedMessage);
            } else {
                return ParallelConsumer.Tuple.pairOf(of(offsetMapPayload), offsetOfNextExpectedMessage);
            }
        } catch (NoEncodingPossibleException e) {
            setAllowedMoreRecords(false);
            log.warn("No encodings could be used to encode the offset map, skipping. Warning: messages might be replayed on rebalance.", e);
            return ParallelConsumer.Tuple.pairOf(empty(), offsetOfNextExpectedMessage);
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
            if (allowedMoreRecords == false) {
                // guard is useful for debugging to catch the transition from false to true
                setAllowedMoreRecords(true);
            }
            log.debug("Payload size {} within threshold {}", metaPayloadLength, getPressureThresholdValue());
        }

        return mustStrip;
    }

    private double getPressureThresholdValue() {
        return DefaultMaxMetadataSize * PartitionStateManager.getUSED_PAYLOAD_THRESHOLD_MULTIPLIER();
    }

    public void onPartitionsRemoved(ShardManager<K, V> sm) {
        sm.removeAnyShardEntriesReferencedFrom(incompleteOffsets.values());
        deregisterMetrics();
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
    private void maybeTruncateOrPruneTrackedOffsets(EpochAndRecordsMap<?, ?>.RecordsAndEpoch polledRecordBatch) {
        var records = polledRecordBatch.getRecords();

        if (records.isEmpty()) {
            log.warn("Polled an empty batch of records? {}", polledRecordBatch);
            return;
        }

        var offsetOfLowestRecord = getFirst(records).get().offset(); // NOSONAR see #isEmpty

        maybeTruncateBelowOrAbove(offsetOfLowestRecord);

        // build the hash set once, so we can do random access checks of our tracked incompletes
        var polledOffsets = records.stream()
                .map(ConsumerRecord::offset)
                .collect(Collectors.toSet());

        var offsetOfHighestRecord = getLast(records).get().offset(); // NOSONAR see #isEmpty

        // for the incomplete offsets within this range of poll batch
        var offsetsToRemoveFromTracking = new ArrayList<Long>();
        var trackedIncompletesWithinPolledBatch = incompleteOffsets.keySet().subSet(offsetOfLowestRecord, true, offsetOfHighestRecord, true);
        for (long trackedIncomplete : trackedIncompletesWithinPolledBatch) {
            boolean incompleteMissingFromPolledRecords = !polledOffsets.contains(trackedIncomplete);

            if (incompleteMissingFromPolledRecords) {
                offsetsToRemoveFromTracking.add(trackedIncomplete);
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
                    offsetOfLowestRecord,
                    offsetOfHighestRecord
            );
            offsetsToRemoveFromTracking.forEach(incompleteOffsets::remove);
        }
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
     * Checks if this record be taken from its partition as work.
     * <p>
     * It checks that the work is not stale, and that the partition ok to allow more records to be processed, or if the
     * record is actually blocking our progress.
     *
     * @return true if this record be taken from its partition as work.
     */
    public boolean couldBeTakenAsWork(WorkContainer<K, V> workContainer) {
        if (checkIfWorkIsStale(workContainer)) {
            log.debug("Work is in queue with stale epoch or no longer assigned. Skipping. Shard it came from will/was removed during partition revocation. WC: {}", workContainer);
            return false;
        } else if (isAllowedMoreRecords()) {
            log.debug("Partition is allowed more records. Taking work. WC: {}", workContainer);
            return true;
        } else if (isBlockingProgress(workContainer)) {
            // allow record to be taken, even if partition is blocked, as this record completion may reduce payload size requirement
            log.debug("Partition is blocked, but this record is blocking progress. Taking work. WC: {}", workContainer);
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

    private void initMetrics() {
        TopicPartition topicPartition = getTp();
        if (topicPartition == null) {
            return;
        }
        Tag[] partitionStateTags = new Tag[]{Tag.of("topic", topicPartition.topic()), Tag.of("partition", String.valueOf(topicPartition.partition()))};
        lastCommittedOffsetGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.PARTITION_LAST_COMMITTED_OFFSET,
                this, partitionState -> partitionState.lastCommittedOffset, partitionStateTags);
        highestSeenOffsetGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.PARTITION_HIGHEST_SEEN_OFFSET,
                this, PartitionState::getOffsetHighestSeen, partitionStateTags);
        highestCompletedOffsetGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.PARTITION_HIGHEST_COMPLETED_OFFSET,
                this, PartitionState::getOffsetHighestSucceeded, partitionStateTags);
        highestSequentialSucceededOffsetGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.PARTITION_HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET,
                this, PartitionState::getOffsetHighestSequentialSucceeded, partitionStateTags);
        numberOfIncompletesGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.PARTITION_INCOMPLETE_OFFSETS,
                this, partitionState -> partitionState.incompleteOffsets.size(), partitionStateTags);
        ephochGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.PARTITION_ASSIGNMENT_EPOCH,
                this, PartitionState::getPartitionsAssignmentEpoch, partitionStateTags);
        ratioMetadataSpaceUsedDistributionSummary = pcMetrics.getDistributionSummaryFromMetricDef(PCMetricsDef.METADATA_SPACE_USED, partitionStateTags);
        ratioPayloadUsedDistributionSummary = pcMetrics.getDistributionSummaryFromMetricDef(PCMetricsDef.PAYLOAD_RATIO_USED, partitionStateTags);
    }

    private void deregisterMetrics() {
        pcMetrics.removeMeter(lastCommittedOffsetGauge);
        pcMetrics.removeMeter(highestSeenOffsetGauge);
        pcMetrics.removeMeter(highestCompletedOffsetGauge);
        pcMetrics.removeMeter(highestSequentialSucceededOffsetGauge);
        pcMetrics.removeMeter(numberOfIncompletesGauge);
        pcMetrics.removeMeter(ephochGauge);
        pcMetrics.removeMeter(ratioMetadataSpaceUsedDistributionSummary);
        pcMetrics.removeMeter(ratioPayloadUsedDistributionSummary);
    }
}
