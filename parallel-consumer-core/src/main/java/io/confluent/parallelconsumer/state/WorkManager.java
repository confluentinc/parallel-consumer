package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.MetricsEvent;
import io.confluent.parallelconsumer.PCMetrics;
import io.confluent.parallelconsumer.PCMetricsTracker;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.*;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;

import static java.lang.Boolean.TRUE;
import static lombok.AccessLevel.PUBLIC;

/**
 * Sharded, prioritised, offset managed, order controlled, delayed work queue.
 * <p>
 * Low Watermark - the highest offset (continuously successful) with all it's previous messages succeeded (the offset
 * one commits to broker)
 * <p>
 * High Water Mark - the highest offset which has succeeded (previous may be incomplete)
 * <p>
 * Highest seen offset - the highest ever seen offset
 * <p>
 * This state is shared between the {@link BrokerPollSystem} thread and the {@link AbstractParallelEoSStreamProcessor}.
 *
 * @author Antony Stubbs
 */
@Slf4j
public class WorkManager<K, V> implements ConsumerRebalanceListener {

    @Getter
    private final ParallelConsumerOptions<K, V> options;

    // todo make private
    @Getter(PUBLIC)
    final PartitionStateManager<K, V> pm;

    // todo make private
    @Getter(PUBLIC)
    private final ShardManager<K, V> sm;

    /**
     * The multiple of {@link ParallelConsumerOptions#getMaxConcurrency()} that should be pre-loaded awaiting
     * processing.
     * <p>
     * We use it here as well to make sure we have a matching number of messages in queues available.
     */
    private final DynamicLoadFactor dynamicLoadFactor;

    @Getter
    private int numberRecordsOutForProcessing = 0;
    private PCModule<K,V> module;
    /**
     * Useful for testing
     */
    @Getter(PUBLIC)
    private final List<Consumer<WorkContainer<K, V>>> successfulWorkListeners = new ArrayList<>();

    public WorkManager(PCModule<K, V> module,
                       DynamicLoadFactor dynamicExtraLoadFactor) {
        this.module = module;
        this.options = module.options();
        this.dynamicLoadFactor = dynamicExtraLoadFactor;
        this.sm = new ShardManager<>(module, this);
        this.pm = new PartitionStateManager<>(module, sm);
    }

    /**
     * Load offset map for assigned partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        pm.onPartitionsAssigned(partitions);
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
        pm.onPartitionsRevoked(partitions);
        onPartitionsRemoved(partitions);
    }

    /**
     * Clear offset map for lost partitions
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        pm.onPartitionsLost(partitions);
        onPartitionsRemoved(partitions);
    }

    void onPartitionsRemoved(final Collection<TopicPartition> partitions) {
        // no-op - nothing to do
    }

    public void registerWork(EpochAndRecordsMap<K, V> records) {
        pm.maybeRegisterNewRecordAsWork(records);
    }

    /**
     * Get work with no limit on quantity, useful for testing.
     */
    public List<WorkContainer<K, V>> getWorkIfAvailable() {
        return getWorkIfAvailable(Integer.MAX_VALUE);
    }

    /**
     * Depth first work retrieval.
     */
    public List<WorkContainer<K, V>> getWorkIfAvailable(final int requestedMaxWorkToRetrieve) {
        // optimise early
        if (requestedMaxWorkToRetrieve < 1) {
            return UniLists.of();
        }

        //
        var work = sm.getWorkIfAvailable(requestedMaxWorkToRetrieve);

        //
        log.debug("Got {} of {} requested records of work. In-flight: {}, Awaiting in commit (partition) queues: {}",
                work.size(),
                requestedMaxWorkToRetrieve,
                getNumberRecordsOutForProcessing(),
                getNumberOfIncompleteOffsets());
        numberRecordsOutForProcessing += work.size();

        return work;
    }
    private Iterable<Tag> computeTagsForWorkContainer(WorkContainer<K,V> wc){
        return Tags.of("partition",String.valueOf(wc.getTopicPartition().partition()),
                "topic", wc.getTopicPartition().topic(),
                "epoch", String.valueOf(wc.getEpoch()));
    }

    public void onSuccessResult(WorkContainer<K, V> wc) {
        log.trace("Work success ({}), removing from processing shard queue", wc);

        module.eventBus().post(MetricsEvent.builder()
                .name(PCMetricsTracker.METRIC_NAME_PROCESSED_RECORDS)
                .value(1.0)
                .tags(Tags.of(computeTagsForWorkContainer(wc))).build());
        wc.endFlight();

        // update as we go
        pm.onSuccess(wc);
        sm.onSuccess(wc);

        // notify listeners
        successfulWorkListeners.forEach(c -> c.accept(wc));

        numberRecordsOutForProcessing--;
    }

    /**
     * Can run from controller or poller thread, depending on which is responsible for committing
     *
     * @see PartitionStateManager#onOffsetCommitSuccess(Map)
     */
    public void onOffsetCommitSuccess(Map<TopicPartition, OffsetAndMetadata> committed) {
        pm.onOffsetCommitSuccess(committed);
    }

    public void onFailureResult(WorkContainer<K, V> wc) {
        // error occurred, put it back in the queue if it can be retried
        module.eventBus().post(MetricsEvent.builder()
                .name(PCMetricsTracker.METRIC_NAME_FAILED_RECORDS)
                .value(1.0)
                .tags(Tags.of(computeTagsForWorkContainer(wc))).build());
        wc.endFlight();
        pm.onFailure(wc);
        sm.onFailure(wc);
        numberRecordsOutForProcessing--;
    }

    public long getNumberOfIncompleteOffsets() {
        return pm.getNumberOfIncompleteOffsets();
    }

    public Map<TopicPartition, OffsetAndMetadata> collectCommitDataForDirtyPartitions() {
        return pm.collectDirtyCommitData();
    }

    /**
     * Have our partitions been revoked? Can a batch contain messages of different epochs?
     *
     * @return true if any epoch is stale, false if not
     * @see #checkIfWorkIsStale(WorkContainer)
     */
    public boolean checkIfWorkIsStale(final List<WorkContainer<K, V>> workContainers) {
        for (final WorkContainer<K, V> workContainer : workContainers) {
            if (checkIfWorkIsStale(workContainer)) return true;
        }
        return false;
    }

    /**
     * Have our partitions been revoked?
     *
     * @return true if epoch doesn't match, false if ok
     */
    public boolean checkIfWorkIsStale(WorkContainer<K, V> workContainer) {
        return pm.getPartitionState(workContainer).checkIfWorkIsStale(workContainer);
    }

    public boolean shouldThrottle() {
        return isSufficientlyLoaded();
    }

    /**
     * @return true if there's enough messages downloaded from the broker already to satisfy the pipeline, false if more
     * should be downloaded (or pipelined in the Consumer)
     */
    public boolean isSufficientlyLoaded() {
        return getNumberOfWorkQueuedInShardsAwaitingSelection() > (long) options.getTargetAmountOfRecordsInFlight() * getLoadingFactor();
    }

    private int getLoadingFactor() {
        return dynamicLoadFactor.getCurrentFactor();
    }

    public boolean workIsWaitingToBeProcessed() {
        return sm.workIsWaitingToBeProcessed();
    }

    public boolean hasWorkInFlight() {
        return getNumberRecordsOutForProcessing() != 0;
    }

    public boolean isWorkInFlightMeetingTarget() {
        return getNumberRecordsOutForProcessing() >= options.getTargetAmountOfRecordsInFlight();
    }

    public long getNumberOfWorkQueuedInShardsAwaitingSelection() {
        return sm.getNumberOfWorkQueuedInShardsAwaitingSelection();
    }

    public boolean hasIncompleteOffsets() {
        return pm.hasIncompleteOffsets();
    }

    public boolean isRecordsAwaitingProcessing() {
        return sm.getNumberOfWorkQueuedInShardsAwaitingSelection() > 0;
    }

    public void handleFutureResult(WorkContainer<K, V> wc) {
        if (checkIfWorkIsStale(wc)) {
            // no op, partition has been revoked
            log.debug("Work result received, but from an old generation. Dropping work from revoked partition {}", wc);
        } else {
            Optional<Boolean> userFunctionSucceeded = wc.getMaybeUserFunctionSucceeded();
            if (userFunctionSucceeded.isPresent()) {
                if (TRUE.equals(userFunctionSucceeded.get())) {
                    onSuccessResult(wc);
                } else {
                    onFailureResult(wc);
                }
            } else {
                throw new IllegalStateException("Work returned, but without a success flag - report a bug");
            }
        }
    }

    public boolean isNoRecordsOutForProcessing() {
        return getNumberRecordsOutForProcessing() == 0;
    }

    public Optional<Duration> getLowestRetryTime() {
        return sm.getLowestRetryTime();
    }

    public boolean isDirty() {
        return pm.isDirty();
    }

    public void addMetricsTo(PCMetrics.PCMetricsBuilder<?, ? extends PCMetrics.PCMetricsBuilder<?, ?>> metrics) {
        metrics.partitionMetrics(getPm().getMetrics());
        metrics.shardMetrics(getSm().getMetrics());
    }

    public void enrichWithIncompletes(Map<TopicPartition, PCMetrics.PCPartitionMetrics> metrics) {
        getPm().enrichWithIncompletes(metrics);
    }
}
