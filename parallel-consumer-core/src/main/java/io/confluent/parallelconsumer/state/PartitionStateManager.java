package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.BrokerPollSystem;
import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * In charge of managing {@link PartitionState}s.
 * <p>
 * This state is shared between the {@link BrokerPollSystem} thread and the {@link AbstractParallelEoSStreamProcessor}.
 *
 * @author Antony Stubbs
 * @see PartitionState
 */
@Slf4j
public class PartitionStateManager<K, V> implements ConsumerRebalanceListener {

    public static final double USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT = 0.75;

    /**
     * Best efforts attempt to prevent usage of offset payload beyond X% - as encoding size test is currently only done
     * per batch, we need to leave some buffer for the required space to overrun before hitting the hard limit where we
     * have to drop the offset payload entirely.
     */
    @Getter
    @Setter
    // todo remove static
    private static double USED_PAYLOAD_THRESHOLD_MULTIPLIER = USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT;

    private final ShardManager<K, V> sm;

    /**
     * Hold the tracking state for each of our managed partitions.
     */
    private final Map<TopicPartition, PartitionState<K, V>> partitionStates = new HashMap<>();

    /**
     * Record the generations of partition assignment, for fencing off invalid work.
     * <p>
     * NOTE: This must live outside of {@link PartitionState}, as it must be tracked across partition lifecycles.
     * <p>
     * Starts at zero.
     * <p>
     * NOTE: Must be concurrent because it can be set by one thread, but read by another.
     */
    private final Map<TopicPartition, Long> partitionsAssignmentEpochs = new ConcurrentHashMap<>();

    private final PCModule<K, V> module;

    public PartitionStateManager(PCModule<K, V> module, ShardManager<K, V> sm) {
        this.sm = sm;
        this.module = module;
    }

    public PartitionState<K, V> getPartitionState(TopicPartition tp) {
        return partitionStates.get(tp);
    }

    private PartitionState<K, V> getPartitionState(EpochAndRecordsMap<K, V>.RecordsAndEpoch recordsAndEpoch) {
        return getPartitionState(recordsAndEpoch.getTopicPartition());
    }

    protected PartitionState<K, V> getPartitionState(WorkContainer<K, V> workContainer) {
        TopicPartition topicPartition = workContainer.getTopicPartition();
        return getPartitionState(topicPartition);
    }

    /**
     * Load offset map for assigned assignedPartitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> assignedPartitions) {
        log.info("Assigned {} total ({} new) partition(s) {}",
                getNumberOfAssignedPartitions(),
                assignedPartitions.size(),
                assignedPartitions);

        for (final TopicPartition partitionKey : assignedPartitions) {
            boolean isAlreadyAssigned = this.partitionStates.containsKey(partitionKey);
            if (isAlreadyAssigned) {
                PartitionState<K, V> previouslyAssignedState = partitionStates.get(partitionKey);
                if (previouslyAssignedState.isRemoved()) {
                    log.trace("Reassignment of previously revoked partition {} - state: {}", partitionKey, previouslyAssignedState);
                } else {
                    log.warn("New assignment of partition which already exists and isn't recorded as removed in " +
                            "partition state. Could be a state bug - was the partition revocation somehow missed, " +
                            "or is this a race? Please file a GH issue. Partition: {}, state: {}", partitionKey, previouslyAssignedState);
                }
            }
        }

        incrementPartitionAssignmentEpoch(assignedPartitions);

        try {
            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(module); // todo remove throw away instance creation - #233
            var partitionStates = om.loadPartitionStateForAssignment(assignedPartitions);
            this.partitionStates.putAll(partitionStates);
        } catch (Exception e) {
            log.error("Error in onPartitionsAssigned", e);
            throw e;
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
            incrementPartitionAssignmentEpoch(partitions);
            resetOffsetMapAndRemoveWork(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsRevoked", e);
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
    public void onOffsetCommitSuccess(Map<TopicPartition, OffsetAndMetadata> committed) {
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
            partition.onPartitionsRemoved(sm);
        }
    }

    /**
     * @return the current epoch of the partition
     */
    public Long getEpochOfPartition(TopicPartition partition) {
        return partitionsAssignmentEpochs.get(partition);
    }

    private void incrementPartitionAssignmentEpoch(final Collection<TopicPartition> partitions) {
        for (final TopicPartition partition : partitions) {
            Long epoch = partitionsAssignmentEpochs.getOrDefault(partition, PartitionState.KAFKA_OFFSET_ABSENCE);
            epoch++;
            partitionsAssignmentEpochs.put(partition, epoch);
        }
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

    public boolean hasIncompleteOffsets() {
        for (var partition : getAssignedPartitions().values()) {
            if (partition.hasIncompleteOffsets())
                return true;
        }
        return false;
    }

    /**
     * @return the number of record entries in all the partitions being managed not yet succeeded in processing
     */
    public long getNumberOfIncompleteOffsets() {
        Collection<PartitionState<K, V>> values = getAssignedPartitions().values();
        return values.stream()
                .mapToLong(PartitionState::getNumberOfIncompleteOffsets)
                .reduce(Long::sum)
                .orElse(0);
    }

    public long getHighestSeenOffset(final TopicPartition tp) {
        return getPartitionState(tp).getOffsetHighestSeen();
    }

    public void onSuccess(WorkContainer<K, V> wc) {
        PartitionState<K, V> partitionState = getPartitionState(wc.getTopicPartition());
        partitionState.onSuccess(wc.offset());
    }

    public void onFailure(WorkContainer<K, V> wc) {
        PartitionState<K, V> partitionState = getPartitionState(wc.getTopicPartition());
        partitionState.onFailure(wc);
    }

    /**
     * Takes a record as work and puts it into internal queues, unless it's been previously recorded as completed as per
     * loaded records.
     */
    void maybeRegisterNewRecordAsWork(final EpochAndRecordsMap<K, V> recordsMap) {
        log.debug("Incoming {} new records...", recordsMap.count());
        for (var recordsAndEpoch : recordsMap.getRecordMap().values()) {
            PartitionState<K, V> partitionState = getPartitionState(recordsAndEpoch);
            partitionState.maybeRegisterNewPollBatchAsWork(recordsAndEpoch);
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> collectDirtyCommitData() {
        var dirties = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (var state : getAssignedPartitions().values()) {
            var offsetAndMetadata = state.getCommitDataIfDirty();
            //noinspection ObjectAllocationInLoop
            offsetAndMetadata.ifPresent(andMetadata -> dirties.put(state.getTp(), andMetadata));
        }
        return dirties;
    }

    private Map<TopicPartition, PartitionState<K, V>> getAssignedPartitions() {
        return Collections.unmodifiableMap(this.partitionStates.entrySet().stream()
                .filter(e -> !e.getValue().isRemoved())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    /**
     * @return true if this record be taken from its partition as work.
     */
    public boolean couldBeTakenAsWork(WorkContainer<K, V> workContainer) {
        return getPartitionState(workContainer)
                .couldBeTakenAsWork(workContainer);
    }

    public boolean isDirty() {
        return this.partitionStates.values().stream()
                .anyMatch(PartitionState::isDirty);
    }
    public long getNumberOfAssignedPartitions() {
        return this.partitionStates.values().stream()
                .filter(
                        x -> {
                            boolean partitionRemoved = x.equals(RemovedPartitionState.getSingleton());
                            return !partitionRemoved;
                        })
                .count();
    }
}
