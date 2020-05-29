package io.confluent.csid.asyncconsumer;

import io.confluent.csid.asyncconsumer.AsyncConsumerOptions.ProcessingOrder;
import io.confluent.csid.utils.WallClock;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

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

    Map<Object, TreeMap<Long, WorkContainer<K, V>>> shardMap = new HashMap<>();

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
        for (ConsumerRecord<K, V> rec : records) {
            Object shardKey = computeShardKey(rec);
            long offset = rec.offset();
            var wc = new WorkContainer<K, V>(rec);

            shardMap.computeIfAbsent(shardKey, (ignore) -> new TreeMap<>())
                    .put(offset, wc);
        }
    }

    private Object computeShardKey(ConsumerRecord<K, V> rec) {
        var key = switch (options.getOrdering()) {
            case KEY -> rec.key();
            case PARTITION, NONE -> new TopicPartition(rec.topic(), rec.partition());
        };
        return key;
    }

    public <R> List<WorkContainer<K, V>> getWork() {
        return getWork(maxInFlight);
    }

    // todo make fair, esp when in no order
    public <R> List<WorkContainer<K, V>> getWork(int max) {
        List<WorkContainer<K, V>> work = new ArrayList<>();

        for (var e : shardMap.entrySet()) {
            if (work.size() >= max)
                break;

            ArrayList<WorkContainer<K, V>> partitionWork = new ArrayList<>();
            var partitionQueue = e.getValue();

            // then iterate over partitionQueue queue
            // todo don't iterate entire stream if limit passed in
            for (var entry : partitionQueue.entrySet()) {
                if (work.size() + partitionWork.size() >= max)
                    break;

                var wc = entry.getValue();
                if (wc.hasDelayPassed(clock) && wc.isNotInFlight()) {
                    wc.takingAsWork();
                    partitionWork.add(wc);
                }

                if (options.getOrdering() == ProcessingOrder.NONE) {
                    // take it - we don't care about processing order, check the next message
                    continue;
                } else {
                    // can't take any more from this partition until this work is finished
                    // processing blocked on this partition, continue to next partition
                    break;
                }
            }
            work.addAll(partitionWork);
        }
        return work;
    }

    public void success(WorkContainer<K, V> wc) {
        log.debug("Work success, removing from queue");
        wc.succeed();
        ConsumerRecord<K, V> cr = wc.getCr();
        Object key = computeShardKey(cr);
        shardMap.get(key).remove(cr.offset());
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
        var queue = shardMap.get(key);
        long offset = wc.getCr().offset();
        queue.put(offset, wc);
    }

    int workRemainingCount() {
        int count = 0;
        for (var e : this.shardMap.entrySet()) {
            count += e.getValue().size();
        }
        return count;
    }

    public WorkContainer<K, V> getWorkContainerForRecord(ConsumerRecord<K, V> rec) {
        Object key = computeShardKey(rec);
        TreeMap<Long, WorkContainer<K, V>> longWorkContainerTreeMap = this.shardMap.get(key);
        long offset = rec.offset();
        WorkContainer<K, V> wc = longWorkContainerTreeMap.get(offset);
        return wc;
    }
}
