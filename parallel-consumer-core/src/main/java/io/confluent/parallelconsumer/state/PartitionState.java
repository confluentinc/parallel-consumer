package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.offsets.EncodingNotSupportedException;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.offsets.OffsetMapCodecManager.DefaultMaxMetadataSize;
import static lombok.AccessLevel.*;

@Slf4j
public class PartitionState<K, V> {

    @Getter
    private final TopicPartition tp;

    /**
     * A subset of Offsets, beyond the highest committable offset, which haven't been totally completed.
     * <p>
     * We only need to know the full incompletes when we do the {@link #findCompletedEligibleOffsetsAndRemove} scan, so
     * find the full sent only then, and discard. Otherwise, for continuous encoding, the encoders track it them
     * selves.
     * <p>
     * We work with incompletes, instead of completes, because it's a bet that most of the time the storage space for
     * storing the incompletes in memory will be smaller.
     *
     * @see #findCompletedEligibleOffsetsAndRemove(boolean)
     * @see #encodeWorkResult(boolean, WorkContainer)
     * @see #onSuccess(WorkContainer)
     */
    // visible for testing
    // todo should be tracked live, as we know when the state of work containers flips - i.e. they are continuously tracked
    // this is derived from partitionCommitQueues WorkContainer states
    private Set<WorkContainer<?, ?>> incompleteOffsets = new ConcurrentSkipListSet<>();

    private final Set<WorkContainer<?, ?>> completedEligibleWork = new ConcurrentSkipListSet<>();

    public Set<Long> getIncompleteOffsets() {
        return incompleteOffsets.parallelStream().map(WorkContainer::offset).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * The highest seen offset for a partition.
     * <p>
     * Starts off as null - no data
     */
    // visible for testing
    @NonNull
    @Getter(PUBLIC)
    private Long offsetHighestSeen;

    /**
     * Highest offset which has completed successfully ("succeeded").
     */
    @Getter(PUBLIC)
    @Setter(PRIVATE)
    private long offsetHighestSucceeded = -1L;

    /**
     * Highest continuous succeeded offset
     */
    @Getter(PUBLIC)
    @Setter(PRIVATE)
    private long offsetHighestSequentialSucceeded = -1L;

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
    @Setter(PACKAGE)
    private boolean allowedMoreRecords = true;

    /**
     * Map of offsets to WorkUnits.
     * <p>
     * Need to record globally consumed records, to ensure correct offset order committal. Cannot rely on incrementally
     * advancing offsets, as this isn't a guarantee of kafka's.
     * <p>
     * Concurrent because either the broker poller thread or the control thread may be requesting offset to commit
     * ({@link #findCompletedEligibleOffsetsAndRemove})
     *
     * @see #findCompletedEligibleOffsetsAndRemove
     */
    private final NavigableMap<Long, WorkContainer<K, V>> commitQueue = new ConcurrentSkipListMap<>();

    NavigableMap<Long, WorkContainer<K, V>> getCommitQueue() {
        return Collections.unmodifiableNavigableMap(commitQueue);
    }

    public PartitionState(TopicPartition tp, OffsetMapCodecManager.HighestOffsetAndIncompletes incompletes) {
        this.tp = tp;
//        this.incompleteOffsets = incompletes.getIncompleteOffsets();
        this.offsetHighestSeen = incompletes.getHighestSeenOffset();
    }

    public void maybeRaiseHighestSeenOffset(final long highestSeen) {
        // rise the high water mark
        Long oldHighestSeen = this.offsetHighestSeen;
        if (oldHighestSeen == null || highestSeen >= oldHighestSeen) {
            log.trace("Updating highest seen - was: {} now: {}", offsetHighestSeen, highestSeen);
            offsetHighestSeen = highestSeen;
        }
    }

    /**
     * Removes all offsets that fall below the new low water mark.
     */
    public void truncateOffsets(final long nextExpectedOffset) {
        incompleteOffsets.removeIf(offset -> offset.offset() < nextExpectedOffset);
    }

    public void onOffsetCommitSuccess(final OffsetPair committed) {
        long nextExpectedOffset = committed.getSync().offset();
        truncateOffsets(nextExpectedOffset);

        // todo possibility this is run a different thread than controller?
        updateIncompletes(committed);
    }

    private void updateIncompletes(OffsetPair committed) {
        // todo - track dirty state here
    }


    boolean isDirty() {
        // todo return is dirty ?
        return true;
    }

    public boolean isRecordPreviouslyCompleted(final ConsumerRecord<K, V> rec) {
        boolean previouslyProcessed;
        long offset = rec.offset();
        if (incompleteOffsets.contains(offset)) {
            // record previously saved as not being completed, can exit early
            previouslyProcessed = false;
        } else {
            Long offsetHighWaterMark = offsetHighestSeen;
            // within the range of tracked offsets, so must have been previously completed
            // we haven't recorded this far up, so must not have been processed yet
            previouslyProcessed = offsetHighWaterMark != null && offset <= offsetHighWaterMark;
        }
        return previouslyProcessed;
    }

    public boolean hasWorkInCommitQueue() {
        return !commitQueue.isEmpty();
    }

    public boolean hasWorkThatNeedsCommitting() {
        return commitQueue.values().parallelStream().anyMatch(x -> x.isUserFunctionSucceeded());
    }

    public int getCommitQueueSize() {
        return commitQueue.size();
    }

    public void onSuccess(WorkContainer<K, V> work) {
        updateHighestSucceededOffsetSoFar(work);
        this.incompleteOffsets.remove(work);
        this.completedEligibleWork.add(work);
    }

    public void onFailure(WorkContainer<K, V> work) {
        this.completedEligibleWork.remove(work);
    }

    /**
     * Update highest Succeeded seen so far
     */
    private void updateHighestSucceededOffsetSoFar(WorkContainer<K, V> work) {
        long highestSucceeded = getOffsetHighestSucceeded();
        long thisOffset = work.offset();
        if (thisOffset > highestSucceeded) {
            log.trace("Updating highest completed - was: {} now: {}", highestSucceeded, thisOffset);
            setOffsetHighestSucceeded(thisOffset);
        }
    }

    public void addWorkContainer(WorkContainer<K, V> wc) {
        maybeRaiseHighestSeenOffset(wc.offset());
        commitQueue.put(wc.offset(), wc);
        incompleteOffsets.add(wc);
    }

    public void remove(Iterable<WorkContainer<?, ?>> workToRemove) {
        for (var workContainer : workToRemove) {
            var offset = workContainer.getCr().offset();
            this.commitQueue.remove(offset);
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

    public void setIncompleteOffsets(LinkedHashSet<Long> incompleteOffsets) {
        //noop delete
    }

    // todo maybe not needed in end
    @Value
    public static class OffsetPair {
        OffsetAndMetadata sync;
        Set<Long> incompletes;
    }

    public Optional<OffsetPair> getCompletedEligibleOffsetsAndRemoveNew() {

//        Set<WorkContainer<?, ?>> completedEligibleOffsetsAndRemoveNewNewNEEEEEW = getCompletedEligibleOffsetsAndRemoveNewNewNEEEEEW(remove);

        return createOffsetMeta().map(x -> new OffsetPair(x, getIncompleteOffsets()));

//        Optional<Long> nextExpectedOffset = getCompletedEligibleOffsetsAndRemoveNewNew(remove);
//
//        return nextExpectedOffset.flatMap(nextOffset ->
//                createOffsetMeta(nextOffset)
//                        .map(x -> new OffsetPair(x, incompleteOffsets)));
    }

    private Optional<OffsetAndMetadata> createOffsetMeta() {
        // try to encode
        Long nextOffset = getNextExpectedPolledOffset();
        Optional<String> payloadOpt = tryToEncodOffsetsStartingAt(nextOffset);
        return payloadOpt.map(payload -> new OffsetAndMetadata(nextOffset, payload))
                .or(() -> Optional.of(new OffsetAndMetadata(nextOffset)));
    }

    private long getNextExpectedPolledOffset() {
        return getOffsetHighestSequentialSucceeded() + 1;
    }

//    private Set<WorkContainer<?, ?>> getCompletedEligibleOffsetsAndRemoveNewNewNEEEEEW(boolean remove) {
//        if (remove) {
//            remove(this.completedEligibleWork);
//        }
//        return completedEligibleWork;
//    }


//    // todo remove parameter remove
//    private Optional<Long> getCompletedEligibleOffsetsAndRemoveNewNew(boolean remove) {
//        //        int count = 0;
////        int removed = 0;
//
//        Map<Long, WorkContainer<K, V>> partitionQueue = getCommitQueue();
//        TopicPartition topicPartitionKey = this.getTp();
//        log.trace("Starting scan of partition: {}", topicPartitionKey);
//
////        count += partitionQueue.size();
//        var workToRemove = new LinkedList<WorkContainer<?, ?>>();
//        var incompleteOffsets = new LinkedHashSet<Long>();
//        long lowWaterMark = -1;
//        var highestSucceeded = getOffsetHighestSucceeded();
//        // can't commit this offset or beyond, as this is the latest offset that is incomplete
//        // i.e. only commit offsets that come before the current one, and stop looking for more
//        boolean beyondSuccessiveSucceededOffsets = false;
//
//        Long nextOffset = null;
//
//        for (final var offsetAndItsWorkContainer : partitionQueue.entrySet()) {
//            // ordered iteration via offset keys thanks to the tree-map
//            WorkContainer<K, V> container = offsetAndItsWorkContainer.getValue();
//
//            //
//            long offset = container.getCr().offset();
//            if (offset > highestSucceeded) {
//                break; // no more to encode
//            }
//
//            //
//            boolean userFuncComplete = container.isUserFunctionComplete();
//            if (userFuncComplete) {
//                Boolean userFuncSuccess = container.getUserFunctionSucceeded().get();
//                if (userFuncSuccess) {
//                    if (beyondSuccessiveSucceededOffsets) {
//                        // todo lookup the low water mark and include here
//                        log.trace("Offset {} is complete and succeeded, but we've iterated past the lowest committable offset ({}). Will mark as complete in the offset map.",
//                                container.getCr().offset(), lowWaterMark);
//                        // no-op - offset map is only for not succeeded or completed offsets
//                    } else {
//                        log.trace("Found offset candidate ({}) to add to offset commit map", container);
//                        workToRemove.add(container);
//                        // as in flights are processed in order, this will keep getting overwritten with the highest offset available
//                        // current offset is the highest successful offset, so commit +1 - offset to be committed is defined as the offset of the next expected message to be read
//                        nextOffset = offset + 1;
//                    }
//                } else {
//                    log.trace("Offset {} is complete, but failed processing. Will track in offset map as failed. Can't do normal offset commit past this point.", container.getCr().offset());
//                    beyondSuccessiveSucceededOffsets = true;
//                    incompleteOffsets.add(offset);
//                }
//            } else {
//                lowWaterMark = container.offset();
//                beyondSuccessiveSucceededOffsets = true;
//                log.trace("Offset (:{}) is incomplete, holding up the queue ({}) of size {}.",
//                        container.getCr().offset(),
//                        topicPartitionKey,
//                        partitionQueue.size());
//                incompleteOffsets.add(offset);
//            }
//
//
//            if (remove) {
////                removed += workToRemove.size();
//                // todo so if work is never put bach???
//                remove(workToRemove);
//            }
//        }
//
//        //
//        // todo smelly - update the partition state with the new found incomplete offsets. This field is used by nested classes accessing the state
//        this.incompleteOffsets = incompleteOffsets;
//
//        return Optional.ofNullable(nextOffset);
//    }


    /**
     * @return the String encoded offset map, if possible
     */
    // todo refactor
    // todo rename TRY to add... or maybe add
    Optional<String> tryToEncodOffsetsStartingAt(Long offsetOfNextExpectedMessage) {
        // TODO potential optimisation: store/compare the current incomplete offsets to the last committed ones, to know if this step is needed or not (new progress has been made) - isdirty?

        if (incompleteOffsets.isEmpty()) {
            setAllowedMoreRecords(true);
            return Optional.empty();
        }

        OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(null); // refactor use of null shouldn't be needed
        String offsetMapPayload;
        try {

            // encode
            offsetMapPayload = om.makeOffsetMetadataPayload(offsetOfNextExpectedMessage, this);

        } catch (EncodingNotSupportedException e) {
            setAllowedMoreRecords(false);
            log.warn("No encodings could be used to encode the offset map, skipping. Warning: messages might be replayed on rebalance.", e);
            return Optional.empty();
        }

        //
        int metaPayloadLength = offsetMapPayload.length();
        boolean moreMessagesAllowed;

        // todo move - move what?
        double pressureThresholdValue = DefaultMaxMetadataSize * PartitionMonitor.getUSED_PAYLOAD_THRESHOLD_MULTIPLIER();

        if (metaPayloadLength > DefaultMaxMetadataSize) {
            // exceeded maximum API allowed, strip the payload
            moreMessagesAllowed = false;
            log.warn("Offset map data too large (size: {}) to fit in metadata payload hard limit of {} - cannot include in commit. " +
                            "Warning: messages might be replayed on rebalance. " +
                            "See kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize = {} and issue #47.",
                    metaPayloadLength, DefaultMaxMetadataSize, DefaultMaxMetadataSize);
        } else if (metaPayloadLength > pressureThresholdValue) { // and thus metaPayloadLength <= DefaultMaxMetadataSize
            // try to turn on back pressure before max size is reached
            moreMessagesAllowed = false;
            log.warn("Payload size {} higher than threshold {}, but still lower than max {}. Will write payload, but will " +
                            "not allow further messages, in order to allow the offset data to shrink (via succeeding messages).",
                    metaPayloadLength, pressureThresholdValue, DefaultMaxMetadataSize);

        } else { // and thus (metaPayloadLength <= pressureThresholdValue)
            moreMessagesAllowed = true;
            log.debug("Payload size {} within threshold {}", metaPayloadLength, pressureThresholdValue);
        }

        setAllowedMoreRecords(moreMessagesAllowed);
        return Optional.of(offsetMapPayload);
    }

}