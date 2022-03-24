package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.BrokerPollSystem;
import io.confluent.parallelconsumer.internal.InternalRuntimeError;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import io.confluent.parallelconsumer.state.PartitionState.OffsetPair;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Clock;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.KafkaUtils.toTopicPartition;
import static io.confluent.csid.utils.StringUtils.msg;

/**
 * In charge of managing {@link PartitionState}s.
 * <p>
 * This state is shared between the {@link BrokerPollSystem} thread and the {@link AbstractParallelEoSStreamProcessor}.
 */
@Slf4j
@RequiredArgsConstructor
// todo rename to partition manager
public class PartitionMonitor<K, V> implements ConsumerRebalanceListener {

    /**
     * Best efforts attempt to prevent usage of offset payload beyond X% - as encoding size test is currently only done
     * per batch, we need to leave some buffer for the required space to overrun before hitting the hard limit where we
     * have to drop the offset payload entirely.
     */
    @Getter
    @Setter
    private static double USED_PAYLOAD_THRESHOLD_MULTIPLIER = 0.75;

    private final Consumer<K, V> consumer;

    private final ShardManager<K, V> sm;

    private final ParallelConsumerOptions<K, V> options;

    /**
     * Hold the tracking state for each of our managed partitions.
     */
    private final Map<TopicPartition, PartitionState<K, V>> partitionStates = new ConcurrentHashMap<>();

    /**
     * Record the generations of partition assignment, for fencing off invalid work.
     * <p>
     * This must live outside of {@link PartitionState}, as it must be tracked across partition lifecycles.
     * <p>
     * Starts at zero.
     */
    private final Map<TopicPartition, Integer> partitionsAssignmentEpochs = new ConcurrentHashMap<>();

    private final Clock clock;

    public PartitionState<K, V> getPartitionState(TopicPartition tp) {
        // may cause the system to wait for a rebalance to finish
        // by locking on partitionState, may cause the system to wait for a rebalance to finish
        synchronized (partitionStates) {
            return partitionStates.get(tp);
        }
    }

    /**
     * Load offset map for assigned partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.debug("Partitions assigned: {}", partitions);
        synchronized (this.partitionStates) {

            for (final TopicPartition partition : partitions) {
                if (this.partitionStates.containsKey(partition)) {
                    PartitionState<K, V> state = partitionStates.get(partition);
                    if (state.isRemoved()) {
                        log.trace("Reassignment of previously revoked partition {} - state: {}", partition, state);
                    } else {
                        log.warn("New assignment of partition which already exists and isn't recorded as removed in " +
                                "partition state. Could be a state bug - was the partition revocation somehow missed, " +
                                "or is this a race? Please file a GH issue. Partition: {}, state: {}", partition, state);
                    }
                }
            }

            incrementPartitionAssignmentEpoch(partitions);

            try {
                Set<TopicPartition> partitionsSet = UniSets.copyOf(partitions);
                OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this.consumer); // todo remove throw away instance creation - #233
                var partitionStates = om.loadOffsetMapForPartition(partitionsSet);
                this.partitionStates.putAll(partitionStates);
            } catch (Exception e) {
                log.error("Error in onPartitionsAssigned", e);
                throw e;
            }

        }
    }

    /**
     * Clear offset map for revoked partitions
     * <p>
     * {@link AbstractParallelEoSStreamProcessor#onPartitionsRevoked} handles committing off offsets upon revoke
     *
     * @see AbstractParallelEoSStreamProcessor#onPartitionsRevoked
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Partitions revoked: {}", partitions);

        try {
            onPartitionsRemoved(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsRevoked", e);
            throw e;
        }
    }

    void onPartitionsRemoved(final Collection<TopicPartition> partitions) {
        synchronized (this.partitionStates) {
            incrementPartitionAssignmentEpoch(partitions);
            resetOffsetMapAndRemoveWork(partitions);
        }
    }

    /**
     * Clear offset map for lost partitions
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        try {
            log.info("Lost partitions: {}", partitions);
            onPartitionsRemoved(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsLost", e);
            throw e;
        }
    }

    /**
     * Truncate our tracked offsets as a commit was successful, so the low water mark rises, and we dont' need to track
     * as much anymore.
     * <p>
     * When commits are made to broker, we can throw away all the individually tracked offsets before the committed
     * offset.
     */
    public void onOffsetCommitSuccess(Map<TopicPartition, OffsetPair> committed) {
        // partitionOffsetHighWaterMarks this will get overwritten in due course
        committed.forEach((tp, meta) -> {
            var partition = getPartitionState(tp);
            partition.onOffsetCommitSuccess(meta);
        });
    }

    /**
     * Remove work from removed partition.
     * <p>
     *
     * <b>On shard removal:</b>
     *
     * <li>{@link  ProcessingOrder#PARTITION} ordering, work shards and partition queues are the same,
     * so remove all from referenced shards
     *
     * <li>{@link ProcessingOrder#KEY} ordering, all records in a shard will be of
     * the same key, so by definition all records with this key should be removed - i.e. the entire shard
     *
     * <li>{@link ProcessingOrder#UNORDERED} ordering, {@link WorkContainer}s go into shards keyed by partition, so
     * falls back to the {@link ProcessingOrder#PARTITION} case
     */
    private void resetOffsetMapAndRemoveWork(Collection<TopicPartition> allRemovedPartitions) {
        for (TopicPartition removedPartition : allRemovedPartitions) {
            // by replacing with a no op implementation, we protect for stale messages still in queues which reference it
            // however it means the map will only grow, but only it's key set
            var partition = this.partitionStates.get(removedPartition);
            partitionStates.put(removedPartition, RemovedPartitionState.getSingleton());

            //
            NavigableMap<Long, WorkContainer<K, V>> workFromRemovedPartition = partition.getCommitQueue();
            sm.removeAnyShardsReferencedBy(workFromRemovedPartition);
        }
    }

    public int getEpoch(final ConsumerRecord<K, V> rec) {
        var tp = toTopicPartition(rec);
        Integer epoch = partitionsAssignmentEpochs.get(tp);
        rec.topic();
        if (epoch == null) {
            throw new InternalRuntimeError(msg("Received message for a partition which is not assigned: {}", rec));
        }
        return epoch;
    }

    private void incrementPartitionAssignmentEpoch(final Collection<TopicPartition> partitions) {
        for (final TopicPartition partition : partitions) {
            int epoch = partitionsAssignmentEpochs.getOrDefault(partition, -1);
            epoch++;
            partitionsAssignmentEpochs.put(partition, epoch);
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
        var topicPartitionKey = workContainer.getTopicPartition();

        Integer currentPartitionEpoch = partitionsAssignmentEpochs.get(topicPartitionKey);
        int workEpoch = workContainer.getEpoch();

        boolean partitionNotAssigned = isPartitionRemovedOrNeverAssigned(workContainer.getCr());

        boolean epochMissMatch = currentPartitionEpoch != workEpoch;

        if (epochMissMatch || partitionNotAssigned) {
            log.debug("Epoch mismatch {} vs {} for record {}. Skipping message - it's partition has already assigned to a different consumer.",
                    workEpoch, currentPartitionEpoch, workContainer);
            return true;
        }
        return false;
    }

    public void maybeRaiseHighestSeenOffset(TopicPartition tp, long seenOffset) {
        PartitionState<K, V> partitionState = getPartitionState(tp);
        partitionState.maybeRaiseHighestSeenOffset(seenOffset);
    }

    boolean isRecordPreviouslyCompleted(ConsumerRecord<K, V> rec) {
        var tp = toTopicPartition(rec);
        var partitionState = getPartitionState(tp);
        boolean previouslyCompleted = partitionState.isRecordPreviouslyCompleted(rec);
        log.trace("Record {} previously completed? {}", rec.offset(), previouslyCompleted);
        return previouslyCompleted;
    }

    /**
     * Check we have capacity in offset storage to process more messages
     */
    public boolean isAllowedMoreRecords(TopicPartition tp) {
        PartitionState<K, V> partitionState = getPartitionState(tp);
        return partitionState.isAllowedMoreRecords();
    }

    /**
     * @see #isAllowedMoreRecords(TopicPartition)
     */
    public boolean isAllowedMoreRecords(WorkContainer<?, ?> wc) {
        return isAllowedMoreRecords(wc.getTopicPartition());
    }

    public boolean hasWorkInCommitQueues() {
        for (var partition : getAssignedPartitions().values()) {
            if (partition.hasWorkInCommitQueue())
                return true;
        }
        return false;
    }

    public long getNumberOfEntriesInPartitionQueues() {
        Collection<PartitionState<K, V>> values = getAssignedPartitions().values();
        return values.stream()
                .mapToLong(PartitionState::getCommitQueueSize)
                .reduce(Long::sum)
                .orElse(0);
    }

    public Optional<Long> getHighestSeenOffset(final TopicPartition tp) {
        return getPartitionState(tp).getOffsetHighestSeen();
    }

    public void addWorkContainer(final WorkContainer<K, V> wc) {
        var tp = wc.getTopicPartition();
        getPartitionState(tp).addWorkContainer(wc);
    }

    /**
     * Checks if partition is blocked with back pressure.
     * <p>
     * If false, more messages are allowed to process for this partition.
     * <p>
     * If true, we have calculated that we can't record any more offsets for this partition, as our best performing
     * encoder requires nearly as much space is available for this partitions allocation of the maximum offset metadata
     * size.
     * <p>
     * Default (missing elements) is true - more messages can be processed.
     *
     * @see OffsetMapCodecManager#DefaultMaxMetadataSize
     */
    public boolean isBlocked(final TopicPartition topicPartition) {
        return !isAllowedMoreRecords(topicPartition);
    }

    public boolean isPartitionRemovedOrNeverAssigned(ConsumerRecord<?, ?> rec) {
        TopicPartition topicPartition = toTopicPartition(rec);
        var partitionState = getPartitionState(topicPartition);
        boolean hasNeverBeenAssigned = partitionState == null;
        return hasNeverBeenAssigned || partitionState.isRemoved();
    }

    public void onSuccess(WorkContainer<K, V> wc) {
        PartitionState<K, V> partitionState = getPartitionState(wc.getTopicPartition());
        partitionState.onSuccess(wc);
    }

    public void onFailure(WorkContainer<K, V> wc) {
        PartitionState<K, V> partitionState = getPartitionState(wc.getTopicPartition());
        partitionState.onFailure(wc);
    }

    /**
     * Takes a record as work and puts it into internal queues, unless it's been previously recorded as completed as per
     * loaded records.
     * <p>
     * Locking on partition state here, means that the check for assignment is in the same sync block as registering the
     * work with the {@link TopicPartition}'s {@link PartitionState} and the {@link ShardManager}. Keeping the two
     * different views in sync. Of course now, having a shared nothing architecture would mean all access to the state
     * is by a single thread, and so this could never occur (see ).
     *
     * @return true if the record was taken, false if it was skipped (previously successful)
     */
    boolean maybeRegisterNewRecordAsWork(final ConsumerRecord<K, V> rec) {
        if (rec == null) return false;

        synchronized (partitionStates) {
            if (isPartitionRemovedOrNeverAssigned(rec)) {
                log.debug("Record in buffer for a partition no longer assigned. Dropping. TP: {} rec: {}", toTopicPartition(rec), rec);
                return false;
            }

            if (isRecordPreviouslyCompleted(rec)) {
                log.trace("Record previously completed, skipping. offset: {}", rec.offset());
                return false;
            } else {
                int currentPartitionEpoch = getEpoch(rec);
                //noinspection unchecked - Lombok builder getter erases generics
                var wc = new WorkContainer<K, V>(currentPartitionEpoch, rec, options.getRetryDelayProvider(), clock);

                sm.addWorkContainer(wc);

                addWorkContainer(wc);

                return true;
            }
        }
    }

    public Map<TopicPartition, OffsetPair> findCompletedEligibleOffsetsAndRemove() {
        return findCompletedEligibleOffsetsAndRemove(true);
    }

    private Map<TopicPartition, OffsetPair> findCompletedEligibleOffsetsAndRemove(boolean remove) {
        var newone = findCompletedEligibleOffsetsAndRemoveNew();
        return newone;
    }

    // todo rename
    private Map<TopicPartition, OffsetPair> findCompletedEligibleOffsetsAndRemoveNew() {
        Map<TopicPartition, OffsetPair> offsetsToSend = new HashMap<>();

        for (var entry : getAssignedPartitions().entrySet()) {

            OffsetPair offsetAndMetadata = entry.getValue().getCompletedEligibleOffsetsAndRemoveNew();

            offsetsToSend.put(entry.getKey(), offsetAndMetadata);
        }

        log.debug("Scan finished, {} were in flight, offset(s) ({}) to be committed", offsetsToSend.size(), offsetsToSend);
        return offsetsToSend;
    }

    private Map<TopicPartition, PartitionState<K, V>> getAssignedPartitions() {
        return Collections.unmodifiableMap(this.partitionStates.entrySet().stream()
                .filter(e -> !e.getValue().isRemoved())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    public boolean couldBeTakenAsWork(WorkContainer<?, ?> workContainer) {
        if (checkIfWorkIsStale(workContainer)) {
            log.debug("Work is in queue with stale epoch or no longer assigned. Skipping. Shard it came from will/was removed during partition revocation. WC: {}", workContainer);
            return false;
        } else if (isAllowedMoreRecords(workContainer)) {
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
        var partitionState = getPartitionState(workContainer.getTopicPartition());
        return workContainer.offset() < partitionState.getOffsetHighestSucceeded();
    }

    boolean isDirty() {
        return this.partitionStates.values().parallelStream().anyMatch(PartitionState::isDirty);
    }

    public boolean isClean() {
        return !isDirty();
    }

}
