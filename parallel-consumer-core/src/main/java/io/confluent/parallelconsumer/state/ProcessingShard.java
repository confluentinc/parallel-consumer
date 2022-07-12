package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.internal.RateLimiter;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.csid.utils.JavaUtils.isGreaterThan;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static lombok.AccessLevel.PRIVATE;

/**
 * Models the queue of work to be processed, based on the {@link ProcessingOrder} modes.
 */
@Slf4j
@RequiredArgsConstructor
public class ProcessingShard<K, V> {

    /**
     * Map of offset to WorkUnits.
     * <p>
     * Uses a ConcurrentSkipListMap instead of a TreeMap as under high pressure there appears to be some concurrency
     * errors (missing WorkContainers). This is addressed in PR#270.
     * <p>
     * Is a Map because need random access into collection, as records don't always complete in order (i.e. UNORDERED
     * mode).
     */
    @Getter
    private final NavigableMap<Long, WorkContainer<K, V>> entries = new ConcurrentSkipListMap<>();

    @Getter(PRIVATE)
    private final ShardKey key;

    private final ParallelConsumerOptions<?, ?> options;

    private final PartitionStateManager<K, V> pm;

    private final RateLimiter slowWarningRateLimit = new RateLimiter(5);

    public boolean workIsWaitingToBeProcessed() {
        return entries.values().parallelStream()
                .anyMatch(kvWorkContainer -> kvWorkContainer.isAvailableToTakeAsWork());
    }

    public void addWorkContainer(WorkContainer<K, V> wc) {
        long key = wc.offset();
        if (entries.containsKey(key)) {
            log.debug("Entry for {} already exists in shard queue, dropping record", wc);
        } else {
            entries.put(key, wc);
        }
    }

    public void onSuccess(WorkContainer<?, ?> wc) {
        // remove work from shard's queue
        entries.remove(wc.offset());
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public long getCountOfWorkAwaitingSelection() {
        return entries.values().stream()
                // todo missing pm.isBlocked(topicPartition) ?
                .filter(WorkContainer::isAvailableToTakeAsWork)
                .count();
    }

    public long getCountOfWorkTracked() {
        return entries.size();
    }

    public long getCountWorkInFlight() {
        return entries.values().stream()
                .filter(WorkContainer::isInFlight)
                .count();
    }

    public WorkContainer<K, V> remove(long offset) {
        return entries.remove(offset);
    }

    ArrayList<WorkContainer<K, V>> getWorkIfAvailable(int workToGetDelta) {
        log.trace("Looking for work on shardQueueEntry: {}", getKey());

        var slowWork = new HashSet<WorkContainer<?, ?>>();
        var workTaken = new ArrayList<WorkContainer<K, V>>();

        var iterator = entries.entrySet().iterator();
        while (workTaken.size() < workToGetDelta && iterator.hasNext()) {
            var workContainer = iterator.next().getValue();

            if (pm.couldBeTakenAsWork(workContainer)) {

                if (workContainer.isAvailableToTakeAsWork()) {
                    log.trace("Taking {} as work", workContainer);
                    workContainer.onQueueingForExecution();
                    workTaken.add(workContainer);
                } else {
                    addToSlowWorkMaybe(slowWork, workContainer);
                }

                if (isOrderRestricted()) {
                    // can't take any more work from this shard, due to ordering restrictions
                    // processing blocked on this shard, continue to next shard
                    log.trace("Processing by {}, so have cannot get more messages on this ({}) shardEntry.", this.options.getOrdering(), getKey());
                    break;
                }
            }
        }

        if (workTaken.size() == workToGetDelta) {
            log.trace("Work taken ({}) exceeds max ({})", workTaken.size(), workToGetDelta);
        }

        logSlowWork(slowWork);

        return workTaken;
    }

    private void logSlowWork(Set<WorkContainer<?, ?>> slowWork) {
        // log
        if (!slowWork.isEmpty()) {
            List<String> slowTopics = slowWork.parallelStream()
                    .map(x -> x.getTopicPartition().toString()).distinct()
                    .collect(Collectors.toList());
            slowWarningRateLimit.performIfNotLimited(() ->
                    log.warn("Warning: {} records in the queue have been waiting longer than {}s for following topics {}.",
                            slowWork.size(), toSeconds(options.getThresholdForTimeSpendInQueueWarning()), slowTopics));
        }
    }

    private void addToSlowWorkMaybe(Set<WorkContainer<?, ?>> slowWork, WorkContainer<?, ?> workContainer) {
        var msgTemplate = "Can't take as work: Work ({}). Must all be true: Delay passed= {}. Is not in flight= {}. Has not succeeded already= {}. Time spent in execution queue: {}.";
        Duration timeInFlight = workContainer.getTimeInFlight();
        var msg = msg(msgTemplate, workContainer, workContainer.hasDelayPassed(), workContainer.isNotInFlight(), !workContainer.isUserFunctionSucceeded(), timeInFlight);
        Duration slowThreshold = options.getThresholdForTimeSpendInQueueWarning();
        if (isGreaterThan(timeInFlight, slowThreshold)) {
            slowWork.add(workContainer);
            log.trace("Work has spent over " + slowThreshold + " in queue! " + msg);
        } else {
            log.trace(msg);
        }
    }

    private boolean isOrderRestricted() {
        return options.getOrdering() != UNORDERED;
    }

}
