package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumer;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.kafka.common.utils.CloseableIterator;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Custom Sorted Set implementation for the retry queue. Difference from standard Sorted Set is that it allows
 * uniqueness constraint to be based on different set of fields than sorting logic. Uniqueness is based on Topic,
 * Partition and Offset of the WorkContainer while sorting is done based on RetryDueAt, Topic, Partition and Offset.
 * <p>
 * To enable that - Set is implemented using two Maps - uniqueness map and sorted map - uniqueness map is used to link
 * the unique keys to sorting keys while sorted map is used to store the sorted elements.
 * <p>
 * Implementation is thread safe and uses ReadWriteLock to allow multiple readers or single writer. Due to use of the
 * locks - it is important to close the Iterator in timely fashion to release the lock and prevent deadlocks.
 * <p>
 * Only a subset of Set methods are implemented - add, remove, clear and iterator - as those are only methods used by
 * the Parallel Consumer code.
 */
public class RetryQueue {

    @Getter(AccessLevel.PACKAGE) //visible for testing
    private final Map<WorkContainerKey, WorkContainerSortKey> unique = new HashMap<>();
    @Getter(AccessLevel.PACKAGE) //visible for testing
    private final NavigableMap<WorkContainerSortKey, WorkContainer<?, ?>> sorted;

    @Getter(AccessLevel.PACKAGE) //visible for testing
    private final Comparator<WorkContainerSortKey> comparator = Comparator
            .comparing(WorkContainerSortKey::getRetryDueAt)
            .thenComparing(WorkContainerKey::getTopic)
            .thenComparing(WorkContainerKey::getPartition)
            .thenComparing(WorkContainerSortKey::getOffset);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public RetryQueue() {
        sorted = new TreeMap<>(comparator);
    }

    /**
     * Get the size of the set
     *
     * @return size of the set
     */
    public int size() {
        return unique.size();
    }

    /**
     * Check if the set is empty
     *
     * @return true if the set is empty
     */
    public boolean isEmpty() {
        return unique.isEmpty();
    }

    /**
     * Check if the set contains a work container - based on Topic, Partition and Offset
     */
    public boolean contains(final WorkContainer<?, ?> wc) {
        lock.readLock().lock();
        try {
            return unique.containsKey(WorkContainerKey.of(wc));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Clear the set
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            unique.clear();
            sorted.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Iterator over the sorted set. Access is guarded by Read lock - so it is really important for it to be closed in
     * timely fashion to release the lock.
     *
     * @return iterator
     */
    public RetryQueueIterator iterator() {
        lock.readLock().lock();
        return new RetryQueueIterator(lock, sorted.values().iterator());
    }

    /**
     * Add a work container to the set. Method follows Set.add() behaviour, returning true if the element was not
     * already present.
     *
     * @param workContainer to add
     * @return true if the element was not already present
     */
    public boolean add(final WorkContainer<?, ?> workContainer) {
        lock.writeLock().lock();
        try {
            WorkContainerKey newKey = WorkContainerKey.of(workContainer);
            WorkContainerSortKey newSortKey = WorkContainerSortKey.of(workContainer);

            WorkContainerSortKey existing = unique.put(newKey, newSortKey);
            if (existing != null) {
                sorted.remove(existing);
            }
            sorted.put(newSortKey, workContainer);
            // interface is set based, so return boolean indicating if element was not present.
            return existing == null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove a work container from the set. Method follows Set.remove() behaviour, returning true if the element was
     * present.
     *
     * @param workContainer
     * @return
     */
    public boolean remove(final WorkContainer<?, ?> workContainer) {
        lock.writeLock().lock();
        try {
            WorkContainerKey newKey = WorkContainerKey.of(workContainer);
            WorkContainerSortKey existing = unique.remove(newKey);
            if (existing != null) {
                sorted.remove(existing);
            }
            return existing != null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove all specified work containers from the set. Method follows Set.removeAll() behaviour, returning true if
     * the set was modified.
     *
     * @param toRemove collection of work containers to remove
     * @return true if the set was modified
     */
    public <K, V> boolean removeAll(List<WorkContainer<K, V>> toRemove) {
        if (toRemove == null || unique.isEmpty()) {
            return false;
        }
        lock.writeLock().lock();
        try {
            List<WorkContainerKey> keysToRemove = toRemove.stream().map(WorkContainerKey::of).collect(Collectors.toList());
            boolean modified = false;
            for (WorkContainerKey wcKey : keysToRemove) {
                WorkContainerSortKey existing = unique.remove(wcKey);
                if (existing != null) {
                    sorted.remove(existing);
                    modified = true;
                }
            }
            return modified;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public WorkContainer<?, ?> last() {
        lock.readLock().lock();
        try {
            return sorted.isEmpty() ? null : sorted.lastEntry().getValue();
        } finally {
            lock.readLock().unlock();
        }
    }

    public WorkContainer<?, ?> first() {
        lock.readLock().lock();
        try {
            return sorted.isEmpty() ? null : sorted.firstEntry().getValue();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns a pair of values - current retry queue size and number of work containers that are ready to be retried
     * Method is combined to provide consistent view of the queue - both values calculated while locked with same read
     * lock preventing racing updates between two reads.
     *
     * @return pair of values - current retry queue size and number of work containers that are ready to be retried
     */
    public ParallelConsumer.Tuple<Integer, Long> getQueueSizeAndNumberReadyToBeRetried() {
        lock.readLock().lock();
        try {
            return new ParallelConsumer.Tuple<>(sorted.size(), getNumberOfFailedWorkReadyToBeRetried());
        } finally {
            lock.readLock().unlock();
        }
    }

    private long getNumberOfFailedWorkReadyToBeRetried() {
        long count = 0;
        //First check if last element is ready to be retried - in that case all before it are ready too
        if (Optional.ofNullable(sorted.isEmpty() ? null : sorted.lastEntry().getValue()).map(WorkContainer::isDelayPassed).orElse(false)) {
            return sorted.size();
        }
        Iterator<WorkContainer<?, ?>> iterator = sorted.values().iterator();
        while (iterator.hasNext()) {
            WorkContainer<?, ?> workContainer = iterator.next();
            //count all work containers that are ready to be retried but not inflight yet
            if (workContainer.isDelayPassed()) {
                count++;
            } else {
                // early stop since retryQueue is sorted by retryDueAt
                break;
            }
        }
        return count;
    }


    @Getter
    @EqualsAndHashCode
    static class WorkContainerKey {
        private final String topic;
        private final Integer partition;
        private final Long offset;

        private WorkContainerKey(String topic, Integer partition, Long offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }

        static WorkContainerKey of(WorkContainer<?, ?> workContainer) {
            return new WorkContainerKey(workContainer.getTopicPartition().topic(),
                    workContainer.getTopicPartition().partition(),
                    workContainer.getCr().offset());
        }
    }

    @Getter
    @EqualsAndHashCode(callSuper = true)
    static class WorkContainerSortKey extends WorkContainerKey {
        private final Instant retryDueAt;

        private WorkContainerSortKey(final String topic, final Integer partition, final Long offset, Instant retryDueAt) {
            super(topic, partition, offset);
            this.retryDueAt = retryDueAt;
        }

        static WorkContainerSortKey of(WorkContainer<?, ?> workContainer) {
            return new WorkContainerSortKey(workContainer.getTopicPartition().topic(),
                    workContainer.getTopicPartition().partition(),
                    workContainer.getCr().offset(),
                    workContainer.getRetryDueAt());
        }
    }

    public static class RetryQueueIterator implements CloseableIterator<WorkContainer<?, ?>> {
        private final ReentrantReadWriteLock lock;
        private final Iterator<WorkContainer<?, ?>> wrapped;
        private boolean closed;

        public RetryQueueIterator(ReentrantReadWriteLock lock, Iterator<WorkContainer<?, ?>> wrapped) {
            this.lock = lock;
            this.wrapped = wrapped;
            this.closed = false;
        }

        @Override
        public void close() {
            lock.readLock().unlock();
            this.closed = true;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                throw new IllegalStateException("RetryQueueIterator is closed");
            }
            return wrapped.hasNext();
        }

        @Override
        public WorkContainer<?, ?> next() {
            if (closed) {
                throw new IllegalStateException("RetryQueueIterator is closed");
            }
            return wrapped.next();
        }
    }
}
