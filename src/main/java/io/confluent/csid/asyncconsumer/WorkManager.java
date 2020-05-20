package io.confluent.csid.asyncconsumer;

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

    Map<TopicPartition, TreeMap<Long, WorkContainer<K, V>>> partitionMap = new HashMap<>();

    private boolean orderedProcessing = false;

    private int maxInFlight;

    // todo for testing - replace with listener or event bus
    @Getter(PACKAGE)
    private List<WorkContainer<K, V>> successfulWork = new ArrayList<>();

    @Setter(PACKAGE)
    private WallClock clock = new WallClock();

    public WorkManager() {
        this(1000);
    }

    public WorkManager(int max) {
        this.maxInFlight = max;
    }

    public void registerWork(ConsumerRecords<K, V> records) {
        for (ConsumerRecord<K, V> rec : records) {
            var tp = new TopicPartition(rec.topic(), rec.partition());
            long offset = rec.offset();
            WorkContainer<K, V> wc = new WorkContainer<>(rec);

            partitionMap.computeIfAbsent(tp, (ignore) -> new TreeMap<>())
                    .put(offset, wc);
        }
    }

    public ArrayList<WorkContainer<K, V>> getWork() {
        return getWork(orderedProcessing, maxInFlight);
    }

    // todo make fair, esp when in no order
    public ArrayList<WorkContainer<K, V>> getWork(boolean ordered, int max) {
        ArrayList<WorkContainer<K, V>> work = new ArrayList<>();

        for (var e : partitionMap.entrySet()) {
            if (work.size() >= max)
                break;

            ArrayList<WorkContainer<K, V>> partitionWork = new ArrayList<>();
            var partitionQueue = e.getValue();

            // then iterate over partitionQueue queue
            // todo don't iterate entire stream if limit passed in
            for (var x : partitionQueue.entrySet()) {
                if (work.size() + partitionWork.size() >= max)
                    break;

                WorkContainer<K, V> wc = x.getValue();
                if (wc.hasDelayPassed(clock) && wc.isNotInFlight()) {
                    wc.takingAsWork();
                    partitionWork.add(wc);
                }

                if (ordered) {
                    // can't take any more from this partition until this work is finished
                    // processing blocked on this partition, continue to next partition
                    break;
                } else {
                    // take it - we don't care about processing order, check the next message
                    continue;
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
        partitionMap.get(toTP(cr)).remove(cr.offset());
        successfulWork.add(wc);
    }

    public List<WorkContainer<K, V>> getTerminallyFailedWork() {
        throw new RuntimeException();
    }

    public void failed(WorkContainer<K, V> wc) {
        wc.fail(clock);
        putBack(wc);
    }

    private void putBack(WorkContainer<K, V> wc) {
        log.debug("Work FAILED, returning to queue");
        ConsumerRecord<K, V> cr = wc.getCr();
        var key = cr.key();
        var tp = toTP(cr);
        TreeMap<Long, WorkContainer<K, V>> queue = partitionMap.get(tp);
        queue.put(wc.getCr().offset(), wc);
    }

    static TopicPartition toTP(ConsumerRecord rec) {
        return new TopicPartition(rec.topic(), rec.partition());
    }

}
