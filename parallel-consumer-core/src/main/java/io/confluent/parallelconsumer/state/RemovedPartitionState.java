package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.KafkaUtils;
import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * No op version of {@link PartitionState} used for when partition assignments are removed, to avoid managing null
 * references or {@link Optional}s. By replacing with a no op implementation, we protect for stale messages still in
 * queues which reference it, among other things.
 * <p>
 * The alternative to this implementation, is having {@link PartitionStateManager#getPartitionState(TopicPartition)}
 * return {@link Optional}, which forces the implicit null check everywhere partition state is retrieved. This was
 * drafted to a degree, but found to be extremely invasive, where this solution with decent separation of concerns and
 * encapsulation, is sufficient and potentially more useful as is non-destructive. Potential issue is that of memory
 * leak as the collection will forever expand. However, even massive partition counts to a single consumer would be in
 * the hundreds of thousands, this would only result in hundreds of thousands of {@link TopicPartition} object keys all
 * pointing to the same instance of {@link RemovedPartitionState}.
 */
@Slf4j
public class RemovedPartitionState<K, V> extends PartitionState<K, V> {

    private static final SortedSet<Long> READ_ONLY_EMPTY_SET = new TreeSet<>();

    public static final String NO_OP = "no-op";

    public static final int NO_EPOCH = -1;

    public RemovedPartitionState(PCModule<K, V> module) {
        super(NO_EPOCH, module, null, OffsetMapCodecManager.HighestOffsetAndIncompletes.of());
    }

    @Override
    public boolean isRemoved() {
        // by definition true in this implementation
        return true;
    }

    @Override
    public TopicPartition getTopicPartition() {
        return null;
    }

    @Override
    public void maybeRegisterNewPollBatchAsWork(@NonNull EpochAndRecordsMap<K, V>.RecordsAndEpoch recordsAndEpoch) {
        // no-op
        log.warn("Dropping polled record batch for partition no longer assigned. WC: {}", recordsAndEpoch);
    }

    /**
     * Don't allow more records to be processed for this partition. Eventually these records triggering this check will
     * be cleaned out.
     *
     * @return always returns false
     */
    @Override
    boolean isAllowedMoreRecords() {
        log.debug(NO_OP);
        return true;
    }

    @Override
    public SortedSet<Long> getIncompleteOffsetsBelowHighestSucceeded() {
        log.debug(NO_OP);
        return READ_ONLY_EMPTY_SET;
    }

    @Override
    public long getOffsetHighestSeen() {
        log.debug(NO_OP);
        return PartitionState.KAFKA_OFFSET_ABSENCE;
    }

    @Override
    public long getOffsetHighestSucceeded() {
        log.debug(NO_OP);
        return PartitionState.KAFKA_OFFSET_ABSENCE;
    }

    @Override
    public boolean isRecordPreviouslyCompleted(final ConsumerRecord<K, V> rec) {
        log.debug("Ignoring previously completed request for partition no longer assigned. Partition: {}", KafkaUtils.toTopicPartition(rec));
        return false;
    }

    @Override
    public boolean hasIncompleteOffsets() {
        return false;
    }

    @Override
    public int getNumberOfIncompleteOffsets() {
        return 0;
    }

    @Override
    public void onSuccess(long offset) {
        log.debug("Dropping completed work container for partition no longer assigned. WC: {}, partition: {}", offset, getTopicPartition());
    }

    @Override
    public boolean isPartitionRemovedOrNeverAssigned() {
        return true;
    }
}
