package io.confluent.parallelconsumer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class WorkMailBoxManager<K, V> {

    private int sharedBoxCount;

    private final LinkedBlockingQueue<ConsumerRecords<K, V>> workInbox = new LinkedBlockingQueue<>();

    private final CountingCRLinkedList<K, V> internalBatchMailQueue = new CountingCRLinkedList<>();

    // TODO when partition state is also refactored, remove Getter
    @Getter
    private final Queue<ConsumerRecord<K, V>> internalFlattenedMailQueue = new LinkedList<>();

    /**
     * @return amount of work queued in the mail box, awaiting processing into shards, not exact
     */
    Integer getWorkQueuedInMailboxCount() {
        return sharedBoxCount +
                internalBatchMailQueue.getNestedCount() +
                internalFlattenedMailQueue.size();
    }

    /**
     * Work must be registered in offset order
     * <p>
     * Thread safe for use by control and broker poller thread.
     *
     * @see WorkManager#success
     * @see WorkManager#raisePartitionHighWaterMark
     */
    public void registerWork(final ConsumerRecords<K, V> records) {
        synchronized (workInbox) {
            sharedBoxCount += records.count();
            workInbox.add(records);
        }
    }

    private void drainSharedMailbox() {
        synchronized (workInbox) {
            workInbox.drainTo(internalBatchMailQueue);
            sharedBoxCount = 0;
        }
    }

    /**
     * Take our inbound messages from the {@link BrokerPollSystem} and add them to our registry.
     *
     * @param requestedMaxWorkToRetrieve
     */
    public void processInbox(final int requestedMaxWorkToRetrieve) {
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


}
