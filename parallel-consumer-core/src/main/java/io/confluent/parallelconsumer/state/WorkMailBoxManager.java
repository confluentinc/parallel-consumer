package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
import io.confluent.parallelconsumer.internal.BrokerPollSystem;
import io.confluent.parallelconsumer.internal.CountingCRLinkedList;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static io.confluent.csid.utils.KafkaUtils.toTopicPartition;

/**
 * Handles the incoming mail for {@link WorkManager}.
 */
@Slf4j
public class WorkMailBoxManager<K, V> {

    /**
     * The number of nested {@link ConsumerRecord} entries in the shared blocking mail box. Cached for performance.
     */
    private int sharedBoxNestedRecordCount;

    /**
     * The shared mailbox. Doesn't need to be thread safe as we already need synchronize on it.
     */
    private final LinkedBlockingQueue<ConsumerRecords<K, V>> workInbox = new LinkedBlockingQueue<>();

    /**
     * Mailbox where mail is transferred to immediately.
     */
    private final CountingCRLinkedList<K, V> internalBatchMailQueue = new CountingCRLinkedList<>();

    /**
     * Queue of records flattened from the {@link #internalBatchMailQueue}.
     * <p>
     * This is needed because {@link java.util.concurrent.BlockingQueue#drainTo(Collection)} must drain to a collection
     * of the same type. We could have {@link BrokerPollSystem} do the flattening, but that would require many calls to
     * the Concurrent queue, where this only needs one. Also as we don't expect there to be that many elements in these
     * collections (as they contain large batches of records), the overhead will be small.
     */
    private final Queue<ConsumerRecord<K, V>> internalFlattenedMailQueue = new LinkedList<>();

    /**
     * @return amount of work queued in the mail box, awaiting processing into shards, not exact
     */
    Integer getAmountOfWorkQueuedWaitingIngestion() {
        return sharedBoxNestedRecordCount +
                internalBatchMailQueue.getNestedCount() +
                internalFlattenedMailQueue.size();
    }

    /**
     * Work must be registered in offset order
     * <p>
     * Thread safe for use by control and broker poller thread.
     *
     * @see WorkManager#onSuccess
     * @see WorkManager#raisePartitionHighWaterMark
     */
    public void registerWork(final ConsumerRecords<K, V> records) {
        synchronized (workInbox) {
            sharedBoxNestedRecordCount += records.count();
            workInbox.add(records);
        }
    }


    /**
     * Must synchronise to keep sharedBoxNestedRecordCount in lock step with the inbox. Register is easy, but drain you
     * need to run through an intermediary collection and then count the nested elements, to know how many to subtract
     * from the Atomic nested count.
     * <p>
     * Plus registering work is relatively infrequent, so shouldn't worry about a little synchronized here - makes it
     * much simpler.
     */
    private void drainSharedMailbox() {
        synchronized (workInbox) {
            workInbox.drainTo(internalBatchMailQueue);
            sharedBoxNestedRecordCount = 0;
        }
    }

    /**
     * Take our inbound messages from the {@link BrokerPollSystem} and add them to our registry.
     */
    private synchronized void flattenBatchQueue() {
        drainSharedMailbox();

        // flatten
        while (!internalBatchMailQueue.isEmpty()) {
            ConsumerRecords<K, V> consumerRecords = internalBatchMailQueue.poll();
            log.debug("Flattening {} records", consumerRecords.count());
            for (final ConsumerRecord<K, V> consumerRecord : consumerRecords) {
                internalFlattenedMailQueue.add(consumerRecord);
            }
        }
    }

    /**
     * Remove revoked work from the mailbox
     */
    public synchronized void onPartitionsRemoved(final Collection<TopicPartition> removedPartitions) {
        log.debug("Removing stale work from inbox queues");
        flattenBatchQueue();
        internalFlattenedMailQueue.removeIf(rec ->
                removedPartitions.contains(toTopicPartition(rec))
        );
    }

    public synchronized boolean internalFlattenedMailQueueIsEmpty() {
        return internalFlattenedMailQueue.isEmpty();
    }

    /**
     * @return the next element in our outbound queue, or null if empty
     */
    public synchronized ConsumerRecord<K, V> internalFlattenedMailQueuePoll() {
        if (internalBatchMailQueue.isEmpty()) {
            // flatten the batch queue in batches when needed
            flattenBatchQueue();
        }
        return internalFlattenedMailQueue.poll();
    }

    public int internalFlattenedMailQueueSize() {
        return internalFlattenedMailQueue.size();
    }
}
