package io.confluent.csid.asyncconsumer;

import io.confluent.csid.asyncconsumer.AsyncConsumerOptions.ProcessingOrder;
import io.confluent.csid.utils.WallClock;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

import static io.confluent.csid.utils.KafkaUtils.toTP;
import static lombok.AccessLevel.PACKAGE;

/**
 * Sharded, prioritised, delayed work queue.
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class WorkManager<K, V> {

    @Getter
    private final AsyncConsumerOptions options;

    // disable if using partition order
    final private Map<Object, TreeMap<Long, WorkContainer<K, V>>> processingShards = new HashMap<>();

    /**
     * need to record globalally consumed records, to ensure correct offset order committal. Cannot rely on
     * incrementally advancing offsets, as this isn't a guarantee of kafka's.
     */
    final private Map<TopicPartition, TreeMap<Long, WorkContainer<K, V>>> partitionRecords = new HashMap<>();

    private int maxInFlight;

    // todo for testing - replace with listener or event bus
    @Getter(PACKAGE)
    private List<WorkContainer<K, V>> successfulWork = new ArrayList<>();

    @Setter(PACKAGE)
    private WallClock clock = new WallClock();

    public WorkManager(int max, AsyncConsumerOptions options) {
        this.maxInFlight = max;
        this.options = options;
    }

    public <R> void registerWork(ConsumerRecords<K, V> records) {
        log.debug("Registering {} records of work", records.count());
        for (ConsumerRecord<K, V> rec : records) {
            Object shardKey = computeShardKey(rec);
            long offset = rec.offset();
            var wc = new WorkContainer<K, V>(rec);

            processingShards.computeIfAbsent(shardKey, (ignore) -> new TreeMap<>())
                    .put(offset, wc);

            partitionRecords.computeIfAbsent(toTP(rec), (ignore) -> new TreeMap<>())
                    .put(offset, wc);
        }
    }

    private Object computeShardKey(ConsumerRecord<K, V> rec) {
        var key = switch (options.getOrdering()) {
            case KEY -> rec.key();
            case PARTITION, UNORDERED -> new TopicPartition(rec.topic(), rec.partition());
        };
        return key;
    }

    public <R> List<WorkContainer<K, V>> getWork() {
        return getWork(maxInFlight);
    }

    // todo make fair, esp when in no order
    public <R> List<WorkContainer<K, V>> getWork(int max) {
        List<WorkContainer<K, V>> work = new ArrayList<>();

        for (var e : processingShards.entrySet()) {
            log.trace("Looking for work on shard: {}", e.getKey());
            if (work.size() >= max)
                break;

            ArrayList<WorkContainer<K, V>> shardWork = new ArrayList<>();
            SortedMap<Long, WorkContainer<K, V>> partitionQueue = e.getValue();

            // then iterate over partitionQueue queue
            // todo don't iterate entire stream if max limit passed in
            Set<Map.Entry<Long, WorkContainer<K, V>>> entries = partitionQueue.entrySet();
            for (var entry : entries) {
                int taken = work.size() + shardWork.size();
                if (taken >= max) {
                    log.trace("Work taken ({}) execedds max ({})", taken, max);
                    break;
                }

                var wc = entry.getValue();
                if (wc.hasDelayPassed(clock) && wc.isNotInFlight()) {
                    log.trace("Taking {} as work", wc);
                    wc.takingAsWork();
                    shardWork.add(wc);
                } else {
                    log.trace("Work ({}) still delayed or is in flight, can't take...", wc);
                }

                if (options.getOrdering() == ProcessingOrder.UNORDERED) {
                    // continue - we don't care about processing order, so check the next message
                    continue;
                } else {
                    // can't take any more from this partition until this work is finished
                    // processing blocked on this partition, continue to next partition
                    log.trace("Processing by {}, so have cannot get more messages on this ({}) shard.", this.options.getOrdering(), e.getKey());
                    break;
                }
            }
            work.addAll(shardWork);
        }
        log.debug("Returning {} records of work", work.size());
        return work;
    }

    public void success(WorkContainer<K, V> wc) {
        ConsumerRecord<K, V> cr = wc.getCr();
        log.trace("Work success ({}), removing from queue", wc);
        wc.succeed();
        Object key = computeShardKey(cr);
        processingShards.get(key).remove(cr.offset());
//        addToCommitQueue(wc);
        successfulWork.add(wc);
    }

    public <R> List<WorkContainer<K, V>> getTerminallyFailedWork() {
        throw new RuntimeException();
    }

    public void failed(WorkContainer<K, V> wc) {
        wc.fail(clock);
        putBack(wc);
    }

    private void putBack(WorkContainer<K, V> wc) {
        log.debug("Work FAILED, returning to queue");
        ConsumerRecord<K, V> cr = wc.getCr();
        Object key = computeShardKey(cr);
        var queue = processingShards.get(key);
        long offset = wc.getCr().offset();
        queue.put(offset, wc);
    }

    int workRemainingCount() {
        int count = 0;
        for (var e : this.processingShards.entrySet()) {
            count += e.getValue().size();
        }
        return count;
    }

    boolean isWorkReamining() {
        return workRemainingCount() > 0;
    }

    public WorkContainer<K, V> getWorkContainerForRecord(ConsumerRecord<K, V> rec) {
        Object key = computeShardKey(rec);
        TreeMap<Long, WorkContainer<K, V>> longWorkContainerTreeMap = this.processingShards.get(key);
        long offset = rec.offset();
        WorkContainer<K, V> wc = longWorkContainerTreeMap.get(offset);
        return wc;
    }

    void addWorkToInFlight(WorkContainer<K, V> work, ConsumerRecord<K, V> cr) {
        final TopicPartition inputTopicPartition = work.getTopicPartition();
        long offset = cr.offset();

        // ensure we have a TreeMap (ordered map by key) for the topic partition we're reading from
        var offsetToFuture = partitionRecords
                .computeIfAbsent(inputTopicPartition, (ignore) -> new TreeMap<>());

        // store the future reference, against it's offset as key
        offsetToFuture.put(offset, work);
    }

    @SneakyThrows
    <R> Map<TopicPartition, OffsetAndMetadata> findCompletedFutureOffsets() {
        Map<TopicPartition, OffsetAndMetadata> offsetsToSend = new HashMap<>();
        int count = 0;
        int removed = 0;
        log.trace("Scanning for in order in-flight work that has completed...");
        for (final var inFlightInPartition : partitionRecords.entrySet()) {
            count += inFlightInPartition.getValue().size();
            var offsetsToRemoveFromInFlight = new LinkedList<Long>();
            TreeMap<Long, WorkContainer<K, V>> inFlightFutures = inFlightInPartition.getValue();
            for (final var offsetAndItsWorkContainer : inFlightFutures.entrySet()) {
                // ordered iteration via offset keys thanks to the tree-map
                WorkContainer<K, V> container = offsetAndItsWorkContainer.getValue();
                boolean complete = container.isComplete();
                if (complete) {
                    long offset = container.getCr().offset();
                    if (container.getUserFunctionSucceeded().get()) {
//                        log.trace("Work completed successfully, so marking to commit");
                        log.trace("Found offset candidate ({}) to add to offset commit map", container);
                        offsetsToRemoveFromInFlight.add(offset);
                        OffsetAndMetadata offsetData = new OffsetAndMetadata(offset, ""); // TODO blank string? move object construction out?
                        TopicPartition topicPartitionKey = toTP(container.getCr());
                        // as in flights are processed in order, this will keep getting overwritten with the highest offset available
                        offsetsToSend.put(topicPartitionKey, offsetData);
                    } else {
                        log.debug("Offset {} is complete, but failed and is holding up the queue. Ending partition scan.", container.getCr().offset());
                        // can't scan any further
                        break;
                    }
                } else {
                    // can't commit this offset or beyond, as this is the latest offset that is incomplete
                    // i.e. only commit offsets that come before the current one, and stop looking for more
                    log.debug("Offset ({}) is incomplete, holding up the queue ({}). Ending partition scan.",
                            container, inFlightInPartition.getKey());
                    break;
                }
            }
            removed += offsetsToRemoveFromInFlight.size();
            for (Long offset : offsetsToRemoveFromInFlight) {
                inFlightFutures.remove(offset);
            }
        }
        log.debug("Scan finished, {} were in flight, {} completed offsets removed, coalesced to {} offset(s) ({}) to be committed",
                count, removed, offsetsToSend.size(), offsetsToSend);
        return offsetsToSend;
    }

}
