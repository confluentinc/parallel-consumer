package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.KafkaUtils;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * No op version of {@link PartitionState} used for when partition assignments are removed, to avoid managing null
 * references or {@link Optional}s. By replacing with a no op implementation, we protect for stale messages still in
 * queues which reference it, among other things.
 * <p>
 * The alternative to this implementation, is having {@link PartitionMonitor#getPartitionState(TopicPartition)} return
 * {@link Optional}, which forces the implicit null check everywhere partition state is retrieved. This was drafted to a
 * degree, but found to be extremely invasive, where this solution with decent separation of concerns and encapsulation,
 * is sufficient and potentially more useful as is non-destructive. Potential issue is that of memory leak as the
 * collection will forever expand. However, even massive partition counts to a single consumer would be in the hundreds
 * of thousands, this would only result in hundreds of thousands of {@link TopicPartition} object keys all pointing to
 * the same instance of {@link RemovedPartitionState}.
 */
@Slf4j
public class RemovedPartitionState<K, V> extends PartitionState<K, V> {

    private static final NavigableMap READ_ONLY_EMPTY_MAP = Collections.unmodifiableNavigableMap(new ConcurrentSkipListMap<>());
    private static final Set READ_ONLY_EMPTY_SET = Collections.unmodifiableSet(new HashSet<>());

    private static final PartitionState singleton = new RemovedPartitionState();
    public static final String NO_OP = "no-op";

    public RemovedPartitionState() {
        super(null, OffsetMapCodecManager.HighestOffsetAndIncompletes.of());
    }

    public static PartitionState getSingleton() {
        return RemovedPartitionState.singleton;
    }

    @Override
    public boolean isRemoved() {
        // by definition true in this implementation
        return true;
    }

    @Override
    public TopicPartition getTp() {
        return null;
    }

    @Override
    public void addWorkContainer(final WorkContainer<K, V> wc) {
        // no-op
        log.warn("Dropping new work container for partition no longer assigned. WC: {}", wc);
    }

    @Override
    public void remove(final LinkedList<WorkContainer<K, V>> workToRemove) {
        if (!workToRemove.isEmpty()) {
            // no-op
            log.debug("Dropping work container to remove for partition no longer assigned. WC: {}", workToRemove);
        }
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
    public Set<Long> getIncompleteOffsets() {
        log.debug(NO_OP);
        //noinspection unchecked - by using unsave generics, we are able to share one static instance
        return READ_ONLY_EMPTY_SET;
    }

    @Override
    public @NonNull Long getOffsetHighestSeen() {
        log.debug(NO_OP);
        return -1L;
    }

    @Override
    public long getOffsetHighestSucceeded() { // NOSONAR
        log.debug(NO_OP);
        return -1L;
    }

    @Override
    NavigableMap<Long, WorkContainer<K, V>> getCommitQueue() {
        //noinspection unchecked - by using unsave generics, we are able to share one static instance
        return READ_ONLY_EMPTY_MAP;
    }

    @Override
    public void setIncompleteOffsets(final Set<Long> incompleteOffsets) {
        log.debug(NO_OP);
    }

    @Override
    void setAllowedMoreRecords(final boolean allowedMoreRecords) {
        log.debug(NO_OP);
    }

    @Override
    public void maybeRaiseHighestSeenOffset(final long highestSeen) {
        log.debug(NO_OP);
    }

    @Override
    public void truncateOffsets(final long nextExpectedOffset) {
        log.debug(NO_OP);
    }

    @Override
    public void onOffsetCommitSuccess(final OffsetAndMetadata committed) {
        log.debug(NO_OP);
    }

    @Override
    public boolean isRecordPreviouslyCompleted(final ConsumerRecord<K, V> rec) {
        log.debug("Ignoring previously completed request for partition no longer assigned. Partition: {}", KafkaUtils.toTopicPartition(rec));
        return false;
    }

    @Override
    public boolean hasWorkInCommitQueue() {
        return false;
    }

    @Override
    public int getCommitQueueSize() {
        return 0;
    }

    @Override
    public void onSuccess(final WorkContainer<K, V> work) {
        log.debug("Dropping completed work container for partition no longer assigned. WC: {}, partition: {}", work, work.getTopicPartition());
    }


}
