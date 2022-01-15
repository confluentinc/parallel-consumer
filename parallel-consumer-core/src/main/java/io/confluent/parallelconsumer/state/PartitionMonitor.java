package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.BrokerPollSystem;
import io.confluent.parallelconsumer.internal.InternalRuntimeError;
import io.confluent.parallelconsumer.offsets.EncodingNotSupportedException;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniSets;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.KafkaUtils.toTopicPartition;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.DefaultMaxMetadataSize;

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
    private double USED_PAYLOAD_THRESHOLD_MULTIPLIER = 0.75;

    private final Consumer<K, V> consumer;

    private final ShardManager<K, V> sm;

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

    /**
     * Gets set to true whenever work is returned completed, so that we know when a commit needs to be made.
     * <p>
     * In normal operation, this probably makes very little difference, as typical commit frequency is 1 second, so low
     * chances no work has completed in the last second.
     */
    private final AtomicBoolean workStateIsDirtyNeedsCommitting = new AtomicBoolean(false);

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
                OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this.consumer); // todo remove throw away instance creation
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
     *
     * @return true if epoch doesn't match, false if ok
     */
    boolean checkIfWorkIsStale(final WorkContainer<K, V> workContainer) {
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

    public boolean isAllowedMoreRecords(TopicPartition tp) {
        PartitionState<K, V> partitionState = getPartitionState(tp);
        return partitionState.isAllowedMoreRecords();
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

    private void setPartitionMoreRecordsAllowedToProcess(TopicPartition topicPartitionKey, boolean moreMessagesAllowed) {
        var state = getPartitionState(topicPartitionKey);
        state.setAllowedMoreRecords(moreMessagesAllowed);
    }

    public Long getHighestSeenOffset(final TopicPartition tp) {
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

    /**
     * Get final offset data, build the offset map, and replace it in our map of offset data to send
     *
     * @param offsetsToSend
     * @param topicPartitionKey
     * @param incompleteOffsets
     */
    //todo refactor
    void addEncodedOffsets(Map<TopicPartition, OffsetAndMetadata> offsetsToSend,
                           TopicPartition topicPartitionKey,
                           LinkedHashSet<Long> incompleteOffsets) {
        // TODO potential optimisation: store/compare the current incomplete offsets to the last committed ones, to know if this step is needed or not (new progress has been made) - isdirty?
        boolean incompleteOffsetsNeedingEncoding = !incompleteOffsets.isEmpty();
        if (incompleteOffsetsNeedingEncoding) {
            // todo offsetOfNextExpectedMessage should be an attribute of State - consider deriving it from the state class
            long offsetOfNextExpectedMessage;
            OffsetAndMetadata finalOffsetOnly = offsetsToSend.get(topicPartitionKey);
            if (finalOffsetOnly == null) {
                // no new low watermark to commit, so use the last one again
                offsetOfNextExpectedMessage = incompleteOffsets.iterator().next(); // first element
            } else {
                offsetOfNextExpectedMessage = finalOffsetOnly.offset();
            }

            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this.consumer);
            try {
                PartitionState<K, V> state = getPartitionState(topicPartitionKey);
                // todo smelly - update the partition state with the new found incomplete offsets. This field is used by nested classes accessing the state
                state.setIncompleteOffsets(incompleteOffsets);
                String offsetMapPayload = om.makeOffsetMetadataPayload(offsetOfNextExpectedMessage, state);
                int metaPayloadLength = offsetMapPayload.length();
                boolean moreMessagesAllowed;
                OffsetAndMetadata offsetWithExtraMap;
                // todo move
                double pressureThresholdValue = DefaultMaxMetadataSize * USED_PAYLOAD_THRESHOLD_MULTIPLIER;

                if (metaPayloadLength > DefaultMaxMetadataSize) {
                    // exceeded maximum API allowed, strip the payload
                    moreMessagesAllowed = false;
                    offsetWithExtraMap = new OffsetAndMetadata(offsetOfNextExpectedMessage); // strip payload
                    log.warn("Offset map data too large (size: {}) to fit in metadata payload hard limit of {} - cannot include in commit. " +
                                    "Warning: messages might be replayed on rebalance. " +
                                    "See kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize = {} and issue #47.",
                            metaPayloadLength, DefaultMaxMetadataSize, DefaultMaxMetadataSize);
                } else if (metaPayloadLength > pressureThresholdValue) { // and thus metaPayloadLength <= DefaultMaxMetadataSize
                    // try to turn on back pressure before max size is reached
                    moreMessagesAllowed = false;
                    offsetWithExtraMap = new OffsetAndMetadata(offsetOfNextExpectedMessage, offsetMapPayload);
                    log.warn("Payload size {} higher than threshold {}, but still lower than max {}. Will write payload, but will " +
                                    "not allow further messages, in order to allow the offset data to shrink (via succeeding messages).",
                            metaPayloadLength, pressureThresholdValue, DefaultMaxMetadataSize);
                } else { // and thus (metaPayloadLength <= pressureThresholdValue)
                    moreMessagesAllowed = true;
                    offsetWithExtraMap = new OffsetAndMetadata(offsetOfNextExpectedMessage, offsetMapPayload);
                    log.debug("Payload size {} within threshold {}", metaPayloadLength, pressureThresholdValue);
                }

                setPartitionMoreRecordsAllowedToProcess(topicPartitionKey, moreMessagesAllowed);
                offsetsToSend.put(topicPartitionKey, offsetWithExtraMap);
            } catch (EncodingNotSupportedException e) {
                setPartitionMoreRecordsAllowedToProcess(topicPartitionKey, false);
                log.warn("No encodings could be used to encode the offset map, skipping. Warning: messages might be replayed on rebalance.", e);
            }
        } else {
            setPartitionMoreRecordsAllowedToProcess(topicPartitionKey, true);
        }
    }


    public boolean isPartitionRemovedOrNeverAssigned(ConsumerRecord<K, V> rec) {
        TopicPartition topicPartition = toTopicPartition(rec);
        PartitionState<K, V> partitionState = getPartitionState(topicPartition);
        boolean hasNeverBeenAssigned = partitionState == null;
        return hasNeverBeenAssigned || partitionState.isRemoved();
    }

    public void onSuccess(WorkContainer<K, V> wc) {
        PartitionState<K, V> partitionState = getPartitionState(wc.getTopicPartition());
        partitionState.onSuccess(wc);
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
                var wc = new WorkContainer<>(currentPartitionEpoch, rec);

                sm.addWorkContainer(wc);

                addWorkContainer(wc);

                return true;
            }
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> findCompletedEligibleOffsetsAndRemove() {
        return findCompletedEligibleOffsetsAndRemove(true);
    }

    /**
     * Finds eligible offset positions to commit in each assigned partition
     */
    // todo remove completely as state of offsets should be tracked live, no need to scan for them - see #201
    // https://github.com/confluentinc/parallel-consumer/issues/201 Refactor: Live tracking of offsets as they change, so we don't need to scan for them
    Map<TopicPartition, OffsetAndMetadata> findCompletedEligibleOffsetsAndRemove(boolean remove) {

        //
        Map<TopicPartition, OffsetAndMetadata> offsetsToSend = new HashMap<>();
        int count = 0;
        int removed = 0;
        log.trace("Scanning for in order in-flight work that has completed...");

        //
        for (var partitionStateEntry : getAssignedPartitions().entrySet()) {
            var partitionState = partitionStateEntry.getValue();
            Map<Long, WorkContainer<K, V>> partitionQueue = partitionState.getCommitQueue();
            TopicPartition topicPartitionKey = partitionStateEntry.getKey();
            log.trace("Starting scan of partition: {}", topicPartitionKey);

            count += partitionQueue.size();
            var workToRemove = new LinkedList<WorkContainer<K, V>>();
            var incompleteOffsets = new LinkedHashSet<Long>();
            long lowWaterMark = -1;
            var highestSucceeded = partitionState.getOffsetHighestSucceeded();
            // can't commit this offset or beyond, as this is the latest offset that is incomplete
            // i.e. only commit offsets that come before the current one, and stop looking for more
            boolean beyondSuccessiveSucceededOffsets = false;
            for (final var offsetAndItsWorkContainer : partitionQueue.entrySet()) {
                // ordered iteration via offset keys thanks to the tree-map
                WorkContainer<K, V> container = offsetAndItsWorkContainer.getValue();

                //
                long offset = container.getCr().offset();
                if (offset > highestSucceeded) {
                    break; // no more to encode
                }

                //
                boolean complete = container.isUserFunctionComplete();
                if (complete) {
                    if (container.getUserFunctionSucceeded().get() && !beyondSuccessiveSucceededOffsets) {
                        log.trace("Found offset candidate ({}) to add to offset commit map", container);
                        workToRemove.add(container);
                        // as in flights are processed in order, this will keep getting overwritten with the highest offset available
                        // current offset is the highest successful offset, so commit +1 - offset to be committed is defined as the offset of the next expected message to be read
                        long offsetOfNextExpectedMessageToBeCommitted = offset + 1;
                        OffsetAndMetadata offsetData = new OffsetAndMetadata(offsetOfNextExpectedMessageToBeCommitted);
                        offsetsToSend.put(topicPartitionKey, offsetData);
                    } else if (container.getUserFunctionSucceeded().get() && beyondSuccessiveSucceededOffsets) {
                        // todo lookup the low water mark and include here
                        log.trace("Offset {} is complete and succeeded, but we've iterated past the lowest committable offset ({}). Will mark as complete in the offset map.",
                                container.getCr().offset(), lowWaterMark);
                        // no-op - offset map is only for not succeeded or completed offsets
                    } else {
                        log.trace("Offset {} is complete, but failed processing. Will track in offset map as failed. Can't do normal offset commit past this point.", container.getCr().offset());
                        beyondSuccessiveSucceededOffsets = true;
                        incompleteOffsets.add(offset);
                    }
                } else {
                    lowWaterMark = container.offset();
                    beyondSuccessiveSucceededOffsets = true;
                    log.trace("Offset (:{}) is incomplete, holding up the queue ({}) of size {}.",
                            container.getCr().offset(),
                            topicPartitionKey,
                            partitionQueue.size());
                    incompleteOffsets.add(offset);
                }
            }

            addEncodedOffsets(offsetsToSend, topicPartitionKey, incompleteOffsets);

            if (remove) {
                removed += workToRemove.size();
                partitionState.remove(workToRemove);
            }
        }


        List<Long> collect = offsetsToSend.values().stream().map(x -> x.offset()).collect(Collectors.toList());
        log.debug("Scan finished, {} were in flight, {} completed offsets removed, coalesced to {} offset(s) ({}) to be committed",
                count, removed, offsetsToSend.size(), offsetsToSend);
        return offsetsToSend;
    }

    private Map<TopicPartition, PartitionState<K, V>> getAssignedPartitions() {
        return Collections.unmodifiableMap(this.partitionStates.entrySet().stream()
                .filter(e -> !e.getValue().isRemoved())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

}
