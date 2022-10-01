package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.internal.*;
import io.confluent.parallelconsumer.internal.EpochAndRecordsMap.RecordsAndEpoch;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.KafkaUtils.toTopicPartition;

/**
 * In charge of managing {@link PartitionState}s.
 * <p>
 * This state is shared between the {@link BrokerPollSystem} thread and the {@link AbstractParallelEoSStreamProcessor}.
 *
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

    private final Consumer<K, V> consumer;

    private final ShardManager<K, V> sm;

    /**
     * Hold the tracking state for each of our managed partitions.
     */
    private final Map<TopicPartition, PartitionState<K, V>> partitionStates = new ConcurrentHashMap<>();

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
        this.consumer = module.consumer();
        this.sm = sm;
        this.module = module;
    }

    public PartitionState<K, V> getPartitionState(TopicPartition tp) {
        return partitionStates.get(tp);
    }

    /**
     * Load offset map for assigned assignedPartitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> assignedPartitions) {
        log.debug("Partitions assigned: {}", assignedPartitions);

        for (final TopicPartition partitionAssignment : assignedPartitions) {
            boolean isAlreadyAssigned = this.partitionStates.containsKey(partitionAssignment);
            if (isAlreadyAssigned) {
                PartitionState<K, V> previouslyAssignedState = partitionStates.get(partitionAssignment);
                if (previouslyAssignedState.isRemoved()) {
                    log.trace("Reassignment of previously revoked partition {} - state: {}", partitionAssignment, previouslyAssignedState);
                } else {
                    log.warn("New assignment of partition which already exists and isn't recorded as removed in " +
                            "partition state. Could be a state bug - was the partition revocation somehow missed, " +
                            "or is this a race? Please file a GH issue. Partition: {}, state: {}", partitionAssignment, previouslyAssignedState);
                }
            }
        }

        incrementPartitionAssignmentEpoch(assignedPartitions);

        try {
            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this.consumer); // todo remove throw away instance creation - #233
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
            onPartitionsRemoved(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsRevoked", e);
            throw e;
        }
    }

    void onPartitionsRemoved(final Collection<TopicPartition> partitions) {
        incrementPartitionAssignmentEpoch(partitions);
        resetOffsetMapAndRemoveWork(partitions);
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
     * @return the current epoch of the partition this record belongs to
     */
    public Long getEpochOfPartitionForRecord(final ConsumerRecord<K, V> rec) {
        var tp = toTopicPartition(rec);
        Long epoch = partitionsAssignmentEpochs.get(tp);
        if (epoch == null) {
            throw new InternalRuntimeError(msg("Received message for a partition which is not assigned: {}", rec));
        }
        return epoch;
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

//    // todo move to partition state
//    public boolean isRecordPreviouslyCompleted(ConsumerRecord<K, V> rec) {
//        var tp = toTopicPartition(rec);
//        var partitionState = getPartitionState(tp);
//        boolean previouslyCompleted = partitionState.isRecordPreviouslyCompleted(rec);
//        log.trace("Record {} previously completed? {}", rec.offset(), previouslyCompleted);
//        return previouslyCompleted;
//    }

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

    public long getHighestSeenOffset(final TopicPartition tp) {
        return getPartitionState(tp).getOffsetHighestSeen();
    }

    // todo move to partition state
    public void addNewIncompleteWorkContainer(final WorkContainer<K, V> wc) {
        var tp = wc.getTopicPartition();
        getPartitionState(tp).addNewIncompleteWorkContainer(wc);
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

    // todo move to partition state
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
     */
    void maybeRegisterNewRecordAsWork(final EpochAndRecordsMap<K, V> recordsMap) {
        log.debug("Incoming {} new records...", recordsMap.count());
        for (var partition : recordsMap.partitions()) {
            RecordsAndEpoch recordsList = recordsMap.records(partition);
            maybeRegisterNewRecordAsWork(recordsList);
        }
    }

    /**
     * @see #maybeRegisterNewRecordAsWork(EpochAndRecordsMap)
     */
    // todo move into PartitionState
    // todo too deep
    // todo inline - shrunk
    private void maybeRegisterNewRecordAsWork(@NonNull RecordsAndEpoch recordsAndEpoch) {
        Long epochOfInboundRecords = recordsAndEpoch.getEpochOfPartitionAtPoll();
        List<ConsumerRecord<K, V>> recordPollBatch = recordsAndEpoch.getRecords();

        if (recordPollBatch.isEmpty()) {
            log.debug("Received empty poll results? {}", recordsAndEpoch);
        } else {
//            // check epoch is ok
//            // todo teach PartitionState to know it's Epoch, move this into PartitionState
//            {
//                final Optional<ConsumerRecord<K, V>> recOpt = getFirst(recordPollBatch);
//                //noinspection OptionalGetWithoutIsPresent -- already checked not empty
//                ConsumerRecord<K, V> sampleRecord = recOpt.get(); // NOSONAR
//                long batchStartOffset = sampleRecord.offset();
//
//                // do epochs still match? do a proactive check, but the epoch will be checked again at work completion as well
//                var currentPartitionEpoch = getEpochOfPartitionForRecord(sampleRecord);
//                boolean epochsDontMatch = !Objects.equals(epochOfInboundRecords, currentPartitionEpoch);
//                if (epochsDontMatch) {
//                    log.debug("Inbound record of work has epoch ({}) not matching currently assigned epoch for the applicable partition ({}), skipping",
//                            epochOfInboundRecords, currentPartitionEpoch);
//                    return;
//                }
//            }

            PartitionState<K, V> partitionState = getPartitionState(partition);
            partitionState.maybeRegisterNewRecordsAsWork(recordsAndEpoch);

//            // todo move to partition state from here, as epoch apparently has to be tracked in PSM
//            if (isPartitionRemovedOrNeverAssigned(sampleRecord)) {
//                log.debug("Record in buffer for a partition no longer assigned. Dropping. TP: {} rec: {}", toTopicPartition(sampleRecord), sampleRecord);
//            } else {
//                //noinspection OptionalGetWithoutIsPresent -- already checked not empty
//                long batchEndOffset = getLast(recordPollBatch).get().offset(); // NOSONAR
//
//                TopicPartition partition = new TopicPartition(sampleRecord.topic(), sampleRecord.partition());
//                partitionState.maybeTruncate(batchStartOffset, batchEndOffset);
//
//                maybeRegisterNewRecordAsWork(epochOfInboundRecords, recordPollBatch);
//            }
        }
    }

//    // todo move to partition state
//    private void maybeRegisterNewRecordAsWork(Long epochOfInboundRecords, List<ConsumerRecord<K, V>> recordPollBatch) {
//        for (var aRecord : recordPollBatch) {
//            if (isRecordPreviouslyCompleted(aRecord)) {
//                log.trace("Record previously completed, skipping. offset: {}", aRecord.offset());
//            } else {
//                //noinspection ObjectAllocationInLoop
//                var work = new WorkContainer<>(epochOfInboundRecords, aRecord, module);
//
//                sm.addWorkContainer(work);
//                addNewIncompleteWorkContainer(work);
//            }
//        }
//    }

    public Map<TopicPartition, OffsetAndMetadata> collectDirtyCommitData() {
        var dirties = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (var state : getAssignedPartitions().values()) {
            var offsetAndMetadata = state.getCommitDataIfDirty();
            offsetAndMetadata.ifPresent(andMetadata -> dirties.put(state.getTp(), andMetadata));
        }
        return dirties;
    }

    private Map<TopicPartition, PartitionState<K, V>> getAssignedPartitions() {
        return Collections.unmodifiableMap(this.partitionStates.entrySet().stream()
                .filter(e -> !e.getValue().isRemoved())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    public boolean couldBeTakenAsWork(WorkContainer<K, V> workContainer) {
        return getPartitionState(workContainer)
                .couldBeTakenAsWork(workContainer);
    }

    private PartitionState<K, V> getPartitionState(WorkContainer<K, V> workContainer) {
        TopicPartition topicPartition = workContainer.getTopicPartition();
        return getPartitionState(topicPartition);
    }


    public boolean isDirty() {
        return this.partitionStates.values().stream()
                .anyMatch(PartitionState::isDirty);
    }
}
