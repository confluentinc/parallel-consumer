package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.InternalRuntimeError;
import io.confluent.parallelconsumer.offsets.EncodingNotSupportedException;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniSets;

import java.util.*;

import static io.confluent.csid.utils.KafkaUtils.toTP;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.DefaultMaxMetadataSize;

/**
 * In charge of managing {@link PartitionState}s.
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

//    private final Object stateLatchLock = new Object();
//    private ReentrantLock stateLock = new ReentrantLock();
//    private CountDownLatch stateLatch = new CountDownLatch(0);
//    private Phaser statePhaser = new Phaser();
//    private Semaphore stateSemaphore = new Semaphore(1, true);

    /**
     * Hold the tracking state for each of our managed partitions.
     */
    // todo make private
    @Getter(AccessLevel.PACKAGE)
    private final Map<TopicPartition, PartitionState<K, V>> states = new HashMap<>();

    /**
     * Record the generations of partition assignment, for fencing off invalid work.
     * <p>
     * This must live outside of {@link PartitionState}, as it must be tracked across partition lifecycles.
     * <p>
     * Starts at zero.
     */
    private final Map<TopicPartition, Integer> partitionsAssignmentEpochs = new HashMap<>();

    @SneakyThrows
    public PartitionState<K, V> getState(TopicPartition tp) {
//        maybeWaitForRebalance();
        // may cause the system to wait for a rebalance to finish
        PartitionState<K, V> kvPartitionState;
        synchronized (states) {
            kvPartitionState = states.get(tp);
        }
        return kvPartitionState;
    }
//
//    @SneakyThrows
//    private void maybeWaitForRebalance() throws InterruptedException {
//        synchronized (stateLatchLock) {
//            if (stateLatch.getCount() > 0)
//                log.debug("State locked for rebalance, waiting...");
//            boolean success = stateLatch.await(60, SECONDS);// todo throw checked?
//            if (!success) throw new TimeoutException("Timeout waiting for rebalance to finish");
//        }
//    }

//    @SneakyThrows
//    private void maybeWaitForRebalance() throws InterruptedException {
//            if (statePhaser.getPhase() > 0)
//                log.debug("State locked for rebalance, waiting...");
//            statePhaser.arriveAndDeregister()
//            statePhaser.awaitAdvance(statePhaser.getPhase())
//            statePhaser.arrive()
//            boolean success = statePhaser.arriveAndAwaitAdvance(60, SECONDS);// todo throw checked?
//            if (!success) throw new TimeoutException("Timeout waiting for rebalance to finish");
//
//    }
//
//    @SneakyThrows
//    private void maybeWaitForRebalance() throws InterruptedException {
//        if (stateSemaphore.availablePermits() < 1)
//            log.debug("State locked for rebalance, waiting...");
////        boolean success = stateSemaphore.tryAcquire(60, SECONDS);// todo throw checked?
//        stateSemaphore.acquireUninterruptibly();
////        if (!success) throw new TimeoutException("Timeout waiting for rebalance to finish");
//        // rebalance finished
//        stateSemaphore.release();
//    }
//
//    @SneakyThrows
//    private void lockState() {
////        synchronized (stateLatchLock) {
////            stateLatch = new CountDownLatch(1);
////        }
//        stateSemaphore.acquireUninterruptibly();
//    }
//
//    private void unlockState() {
////        stateLatch.countDown();
//        stateSemaphore.release();
//    }

    /**
     * Load offset map for assigned partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.debug("Partitions assigned: {}", partitions);
//        lockState();
        synchronized (states) {

            for (final TopicPartition partition : partitions) {
                if (states.containsKey(partition))
                    log.warn("New assignment of partition {} which already exists in partition state. Could be a state bug.", partition);

            }

            incrementPartitionAssignmentEpoch(partitions);

            try {
                Set<TopicPartition> partitionsSet = UniSets.copyOf(partitions);
                OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this.consumer); // todo remove throw away instance creation
                var partitionStates = om.loadOffsetMapForPartition(partitionsSet);
                states.putAll(partitionStates);
            } catch (Exception e) {
                log.error("Error in onPartitionsAssigned", e);
                throw e;
            }

            // todo delete
//        wm.raisePartitionHighWaterMark(tp, incompletes.getHighestSeenOffset());

//        unlockState();
        }
    }

    /**
     * Clear offset map for revoked partitions
     * <p>
     * {@link ParallelEoSStreamProcessor#onPartitionsRevoked} handles committing off offsets upon revoke
     *
     * @see ParallelEoSStreamProcessor#onPartitionsRevoked
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
//        lockState();
        synchronized (states) {
            incrementPartitionAssignmentEpoch(partitions);
            resetOffsetMapAndRemoveWork(partitions);
        }
//        unlockState();
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
    public void onOffsetCommitSuccess(Map<TopicPartition, OffsetAndMetadata> offsetsToSend) {
        // partitionOffsetHighWaterMarks this will get overwritten in due course
        offsetsToSend.forEach((tp, meta) -> {
            var partition = getState(tp);
            partition.onOffsetCommitSuccess(meta);
        });
    }

    private void resetOffsetMapAndRemoveWork(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            var state = states.remove(partition);

            //
            NavigableMap<Long, WorkContainer<K, V>> oldWorkPartitionQueue = state.getPartitionCommitQueues();
            if (oldWorkPartitionQueue != null) {
                sm.removeShardsFoundIn(oldWorkPartitionQueue);
            } else {
                log.trace("Removing empty commit queue");
            }
        }
    }

    // todo remove tp - derived from rec
    public int getEpoch(final ConsumerRecord<K, V> rec, final TopicPartition tp) {
        Integer epoch = partitionsAssignmentEpochs.get(tp);
        rec.topic();
        if (epoch == null) {
            throw new InternalRuntimeError(msg("Received message for a partition which is not assigned: {}", rec));
        }
        return epoch;
    }

    public int getEpoch(final ConsumerRecord<K, V> rec) {
        Integer epoch = partitionsAssignmentEpochs.get(toTP(rec));
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
    boolean checkEpochIsStale(final WorkContainer<K, V> workContainer) {
        var topicPartitionKey = workContainer.getTopicPartition();

        Integer currentPartitionEpoch = partitionsAssignmentEpochs.get(topicPartitionKey);
        int workEpoch = workContainer.getEpoch();
        if (currentPartitionEpoch != workEpoch) {
            log.debug("Epoch mismatch {} vs {} for record {} - were partitions lost? Skipping message - it's already assigned to a different consumer (possibly me).",
                    workEpoch, currentPartitionEpoch, workContainer);
            return true;
        }
        return false;
    }

    // todo reduce visibility
    public void raisePartitionHighWaterMark(TopicPartition tp, long highWater) {
        getState(tp).risePartitionHighWaterMark(highWater);
    }

    boolean isRecordPreviouslyProcessed(ConsumerRecord<K, V> rec) {
        var tp = toTP(rec);
        var partition = getState(tp);
        if (partition == null) {
            // todo delete block - should never get here
            int epoch = getEpoch(rec, tp);
            log.error("No state found for partition {}, presuming message never before been processed. Partition epoch: {}", tp, epoch);
            return false;
        }
        boolean previouslyProcessed = partition.isRecordPreviouslyProcessed(rec);
        log.trace("Record {} previously seen? {}", rec.offset(), previouslyProcessed);
        return previouslyProcessed;
    }

    // todo terrible name - rename, reduce visibility
    public boolean isAllowedMoreRecords(TopicPartition tp) {
        return getState(tp).isAllowedMoreRecords();
    }

    public boolean hasWorkInCommitQueues() {
        for (var partition : this.states.values()) {
            if (partition.hasWorkInCommitQueue())
                return true;
        }
        return false;
    }

    public long getNumberOfEntriesInPartitionQueues() {
        //states.values().stream().collect(x->x.getCommitQueueSize());
//        int count = 0;
//        for (var e : this.states.values()) {
//            count += e.getCommitQueueSize();
//        }
//        return count;

        // todo is this worse?
        return states.values().stream()
                .mapToLong(PartitionState::getCommitQueueSize)
                .reduce(Long::sum).orElse(0);
    }

    // todo can set private? - package private to work manager
    void setPartitionMoreRecordsAllowedToProcess(TopicPartition topicPartitionKey, boolean moreMessagesAllowed) {
        var state = getState(topicPartitionKey);
        //state.partitionMoreRecordsAllowedToProcess = moreMessagesAllowed;
        state.setAllowedMoreRecords(moreMessagesAllowed);
    }

    public Long getHighWaterMark(final TopicPartition tp) {
        return getState(tp).getPartitionOffsetHighWaterMarks();
    }

    public void addWorkContainer(final WorkContainer<K, V> wc) {
        TopicPartition tp = wc.getTopicPartition();
        NavigableMap<Long, WorkContainer<K, V>> queue = getState(tp).getPartitionCommitQueues();
        queue.put(wc.offset(), wc);
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
     * Get final offset data, build the the offset map, and replace it in our map of offset data to send
     *
     * @param offsetsToSend
     * @param topicPartitionKey
     * @param incompleteOffsets
     * @return
     */
    //todo refactor
    void addEncodedOffsets(Map<TopicPartition, OffsetAndMetadata> offsetsToSend,
                           TopicPartition topicPartitionKey,
                           LinkedHashSet<Long> incompleteOffsets) {
        // TODO potential optimisation: store/compare the current incomplete offsets to the last committed ones, to know if this step is needed or not (new progress has been made) - isdirty?
        boolean offsetEncodingNeeded = !incompleteOffsets.isEmpty();
        if (offsetEncodingNeeded) {
            // todo offsetOfNextExpectedMessage should be an attribute of State - consider deriving it from the state class
            long offsetOfNextExpectedMessage;
            OffsetAndMetadata finalOffsetOnly = offsetsToSend.get(topicPartitionKey);
            if (finalOffsetOnly == null) {
                // no new low water mark to commit, so use the last one again
                offsetOfNextExpectedMessage = incompleteOffsets.iterator().next(); // first element
            } else {
                offsetOfNextExpectedMessage = finalOffsetOnly.offset();
            }

            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this.consumer);
            try {
                PartitionState<K, V> state = getState(topicPartitionKey);
                // todo smelly - update the partition state with the new found incomplete offsets. This field is used by nested classes accessing the state
                state.setPartitionIncompleteOffsets(incompleteOffsets);
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

    public boolean isPartitionAssigned(ConsumerRecord<K, V> rec) {
        return (getState(toTP(rec)) != null);
    }
}
