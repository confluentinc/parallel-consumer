package io.confluent.csid.asyncconsumer;

import io.confluent.csid.utils.KafkaUtils;
import io.confluent.csid.utils.WallClock;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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

    public <R> void registerWork(ConsumerRecords<K, V> records) {
        for (ConsumerRecord<K, V> rec : records) {
            var tp = new TopicPartition(rec.topic(), rec.partition());
            long offset = rec.offset();
            var wc = new WorkContainer<K, V>(rec);

            partitionMap.computeIfAbsent(tp, (ignore) -> new TreeMap<>())
                    .put(offset, wc);
        }
    }

    public <R> List<WorkContainer<K, V>> getWork() {
        return getWork(orderedProcessing, maxInFlight);
    }

    // todo make fair, esp when in no order
    public <R> List<WorkContainer<K, V>> getWork(boolean ordered, int max) {
        List<WorkContainer<K, V>> work = new ArrayList<>();

        for (var e : partitionMap.entrySet()) {
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
        ConsumerRecord<?, ?> cr = wc.getCr();
        partitionMap.get(toTP(cr)).remove(cr.offset());
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
        ConsumerRecord<?, ?> cr = wc.getCr();
        var tp = toTP(cr);
        var queue = partitionMap.get(tp);
        long offset = wc.getCr().offset();
        queue.put(offset, wc);
    }

    int workRemainingCount() {
        int count = 0;
        for (var e : this.partitionMap.entrySet()) {
            count += e.getValue().size();
        }
        return count;
    }

    public WorkContainer<K, V> getWorkContainerForRecord(ConsumerRecord<K, V> rec) {
        TopicPartition topicPartition = toTP(rec);
        TreeMap<Long, WorkContainer<K, V>> longWorkContainerTreeMap = this.partitionMap.get(topicPartition);
        long offset = rec.offset();
        WorkContainer<K, V> wc = longWorkContainerTreeMap.get(offset);
        return wc;
    }
}
