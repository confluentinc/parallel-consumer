package io.confluent.csid.asyncconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.asyncconsumer.AsyncConsumerOptions.ProcessingOrder;
import io.confluent.csid.utils.LoopingResumingIterator;
import io.confluent.csid.utils.Range;
import io.confluent.csid.utils.WallClock;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniLists;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;

import static io.confluent.csid.utils.KafkaUtils.toTP;
import static java.lang.Math.min;
import static lombok.AccessLevel.PACKAGE;

/**
 * Sharded, prioritised, offset managed, order controlled, delayed work queue.
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class WorkManager<K, V> {

    @Getter
    private final AsyncConsumerOptions options;

    // todo performance: disable/remove if using partition order
    final private Map<Object, NavigableMap<Long, WorkContainer<K, V>>> processingShards = new ConcurrentHashMap<>();

    /**
     * Need to record globally consumed records, to ensure correct offset order committal. Cannot rely on incrementally
     * advancing offsets, as this isn't a guarantee of kafka's.
     */
    final private Map<TopicPartition, NavigableMap<Long, WorkContainer<K, V>>> partitionCommitQueues = new ConcurrentHashMap<>();

    /**
     * Iteration resume point, to ensure fairness (prevent shard starvation) when we can't process messages from every
     * shard.
     */
    private Optional<Object> iterationResumePoint = Optional.empty();

    private int inFlightCount = 0;

    /**
     * The multiple of {@link AsyncConsumerOptions#getMaxConcurrency()} that should be pre-loaded awaiting processing.
     * Consumer already pipelines, so we shouldn't need to pipeline ourselves too much.
     */
    private final int loadingFactor = 2;

    /**
     * Useful for testing
     */
    @Getter(PACKAGE)
    private final List<Consumer<WorkContainer<K, V>>> successfulWorkListeners = new ArrayList<>();

    @Setter(PACKAGE)
    private WallClock clock = new WallClock();

    public WorkManager(AsyncConsumerOptions options, org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        this.options = options;

        loadOffsetMap(consumer);
    }

    /**
     * Load all the previously completed offsets that were not committed
     */
    private void loadOffsetMap(org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        Set<TopicPartition> assignment = consumer.assignment();
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(assignment);
        committed.forEach((tp, offsetAndMeta) -> {
            long offset = offsetAndMeta.offset();
            String metadata = offsetAndMeta.metadata();
            loadOffsetMetadataPayload(tp, metadata);
        });
    }

    public void registerWork(List<ConsumerRecords<K, V>> records) {
        for (var record : records) {
            registerWork(record);
        }
    }

    public void registerWork(ConsumerRecords<K, V> records) {
        log.debug("Registering {} records of work", records.count());
        for (ConsumerRecord<K, V> rec : records) {
            if (!recordPreviouslyProcessed(rec)) {
                Object shardKey = computeShardKey(rec);
                long offset = rec.offset();
                var wc = new WorkContainer<K, V>(rec);

                processingShards.computeIfAbsent(shardKey, (ignore) -> new ConcurrentSkipListMap<>())
                        .put(offset, wc);

                partitionCommitQueues.computeIfAbsent(toTP(rec), (ignore) -> new ConcurrentSkipListMap<>())
                        .put(offset, wc);
            }
        }
    }

    Map<TopicPartition, TreeSet<Long>> incompleteOffsets = new HashMap<>();
    Map<TopicPartition, Long> partitionOffsetHighWaterMarks = new HashMap<>();

    private boolean recordPreviouslyProcessed(ConsumerRecord<K, V> rec) {
        long offset = rec.offset();
        TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
        if (incompleteOffsets.contains(offset)) {
            // record previously saved as having not been processed
            return false;
        } else {
            Long offsetHighWaterMarks = partitionOffsetHighWaterMarks.get(tp);
            if (offset < offsetHighWaterMarks) {
                // within the range of tracked offsets, so must have been previously completed
                return true;
            } else {
                // we haven't recorded this far up, so must not have been processed yet
                return false;
            }
        }
    }

    private Object computeShardKey(ConsumerRecord<K, V> rec) {
        return switch (options.getOrdering()) {
            case KEY -> rec.key();
            case PARTITION, UNORDERED -> new TopicPartition(rec.topic(), rec.partition());
        };
    }

    public <R> List<WorkContainer<K, V>> maybeGetWork() {
        return maybeGetWork(options.getMaxConcurrency());
    }

    /**
     * Depth first work retrieval.
     *
     * @param requestedMaxWorkToRetrieve ignored unless less than {@link AsyncConsumerOptions#getMaxConcurrency()}
     */
    public List<WorkContainer<K, V>> maybeGetWork(int requestedMaxWorkToRetrieve) {
        int minWorkToGetSetting = min(min(requestedMaxWorkToRetrieve, options.getMaxConcurrency()), options.getMaxUncommittedMessagesToHandle());
        int workToGetDelta = minWorkToGetSetting - getInFlightCount();

        // optimise early
        if (workToGetDelta < 1) {
            return UniLists.of();
        }

        //
        List<WorkContainer<K, V>> work = new ArrayList<>();

        //
        var it = new LoopingResumingIterator<>(iterationResumePoint, processingShards);

        //
        for (var shard : it) {
            log.trace("Looking for work on shard: {}", shard.getKey());
            if (work.size() >= workToGetDelta) {
                this.iterationResumePoint = Optional.of(shard.getKey());
                log.debug("Work taken is now over max, stopping (saving iteration resume point {})", iterationResumePoint);
                break;
            }

            ArrayList<WorkContainer<K, V>> shardWork = new ArrayList<>();
            SortedMap<Long, WorkContainer<K, V>> shardQueue = shard.getValue();

            // then iterate over shardQueue queue
            Set<Map.Entry<Long, WorkContainer<K, V>>> shardQueueEntries = shardQueue.entrySet();
            for (var queueEntry : shardQueueEntries) {
                int taken = work.size() + shardWork.size();
                if (taken >= workToGetDelta) {
                    log.trace("Work taken ({}) exceeds max ({})", taken, workToGetDelta);
                    break;
                }

                var wc = queueEntry.getValue();
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
                    log.trace("Processing by {}, so have cannot get more messages on this ({}) shard.", this.options.getOrdering(), shard.getKey());
                    break;
                }
            }
            work.addAll(shardWork);
        }

        log.debug("Returning {} records of work", work.size());
        inFlightCount += work.size();
        return work;
    }

    public void success(WorkContainer<K, V> wc) {
        ConsumerRecord<K, V> cr = wc.getCr();
        log.trace("Work success ({}), removing from queue", wc);
        wc.succeed();
        Object key = computeShardKey(cr);
        processingShards.get(key).remove(cr.offset());
        successfulWorkListeners.forEach((c) -> c.accept(wc)); // notify listeners
        inFlightCount--;
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
        inFlightCount--;
    }

    public int getPartitionWorkRemainingCount() {
        int count = 0;
        for (var e : this.partitionCommitQueues.entrySet()) {
            count += e.getValue().size();
        }
        return count;
    }

    public int getShardWorkRemainingCount() {
        int count = 0;
        for (var e : this.processingShards.entrySet()) {
            count += e.getValue().size();
        }
        return count;
    }

    boolean isWorkRemaining() {
        return getPartitionWorkRemainingCount() > 0;
    }

    public WorkContainer<K, V> getWorkContainerForRecord(ConsumerRecord<K, V> rec) {
        Object key = computeShardKey(rec);
        var longWorkContainerTreeMap = this.processingShards.get(key);
        long offset = rec.offset();
        WorkContainer<K, V> wc = longWorkContainerTreeMap.get(offset);
        return wc;
    }

    Map<TopicPartition, OffsetAndMetadata> findCompletedEligibleOffsetsAndRemove() {
        return findCompletedEligibleOffsetsAndRemove(true);
    }

    boolean hasComittableOffsets() {
        return findCompletedEligibleOffsetsAndRemove(false).size() != 0;
    }

    @SneakyThrows
    <R> Map<TopicPartition, OffsetAndMetadata> findCompletedEligibleOffsetsAndRemove(boolean remove) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToSend = new HashMap<>();
        int count = 0;
        int removed = 0;
        log.trace("Scanning for in order in-flight work that has completed...");
        for (final var partitionQueueEntry : partitionCommitQueues.entrySet()) {
            var partitionQueue = partitionQueueEntry.getValue();
            count += partitionQueue.size();
            var workToRemove = new LinkedList<WorkContainer<K, V>>();
            Set<Long> incompleteOffsets = new HashSet<>();
            for (final var offsetAndItsWorkContainer : partitionQueue.entrySet()) {
                // ordered iteration via offset keys thanks to the tree-map
                WorkContainer<K, V> container = offsetAndItsWorkContainer.getValue();
                boolean complete = container.isComplete();
                long offset = container.getCr().offset();
                TopicPartition topicPartitionKey = toTP(container.getCr());
                if (complete) {
                    if (container.getUserFunctionSucceeded().get()) {
                        log.trace("Found offset candidate ({}) to add to offset commit map", container);
                        workToRemove.add(container);
                        // as in flights are processed in order, this will keep getting overwritten with the highest offset available
                        OffsetAndMetadata offsetData = new OffsetAndMetadata(offset);
                        offsetsToSend.put(topicPartitionKey, offsetData);
                    } else {
                        log.debug("Offset {} is complete, but failed and is holding up the queue. Ending partition scan.", container.getCr().offset());
                        incompleteOffsets.add(offset);
                    }
                } else {
                    // can't commit this offset or beyond, as this is the latest offset that is incomplete
                    // i.e. only commit offsets that come before the current one, and stop looking for more
                    log.debug("Offset ({}) is incomplete, holding up the queue ({}) of size {}. Ending partition scan.",
                            container, partitionQueueEntry.getKey(), partitionQueueEntry.getValue().size());
                    incompleteOffsets.add(offset);
                }
                // incomplete offset map
                String offsetMap = serialiseIncompleteOffsetMap(incompleteOffsets);
                OffsetAndMetadata offsetOnly = offsetsToSend.get(topicPartitionKey);
                new OffsetAndMetadata(offsetOnly.offset(), offsetMap);
            }
            if (remove) {
                removed += workToRemove.size();
                for (var w : workToRemove) {
                    var offset = w.getCr().offset();
                    partitionQueue.remove(offset);
                }
            }
        }
        log.debug("Scan finished, {} were in flight, {} completed offsets removed, coalesced to {} offset(s) ({}) to be committed",
                count, removed, offsetsToSend.size(), offsetsToSend);
        return offsetsToSend;
    }

    @SneakyThrows
    Set<Long> deserialiseIncompleteOffsetMap(String incompleteOffsetMap) {
        byte[] decode = Base64.getDecoder().decode(incompleteOffsetMap);
        ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(decode));
        Set<Long> incompleteOffsets = (Set<Long>) objectInputStream.readObject();
//        new InputStream(incompleteOffsetMap)
//        try (ObjectInputStream in = new ObjectInputStream(inputStream)) {
//            @SuppressWarnings("unchecked")
//            final T obj = (T) in.readObject();
//            return obj;
//        } catch (final ClassNotFoundException | IOException ex) {
//            throw new SerializationException(ex);
//        }
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        ObjectOutputStream os = new ObjectOutputStream(baos);
//        os.writeObject(incompleteOffsets);
//        os.close();
//        return baos.toString(StandardCharsets.UTF_8);
        return incompleteOffsets;
    }
//
//    Set<Long> makeCompletedOffsets(Set<Long> incompleteOffsets, int startOffset, int endOffset) {
//        Set<Long> completeOffsets = new HashSet<Long>();
//        Range.range(startOffset, endOffset).foreach {
//            x ->
//            if (incompleteOffsets.contains(x)) {
//                // do nothing
//            } else {
//                completeOffsets.add(x);
//            }
//        }
//        return completeOffsets;
//    }

    void loadOffsetMetadataPayload(TopicPartition tp, String offsetMetadataPayload) {
        String[] split = offsetMetadataPayload.split(",");
        long offset = Long.parseLong(split[0]);
        this.partitionOffsetHighWaterMarks.put(tp, offset);
        Set<Long> longs = deserialiseIncompleteOffsetMap(split[1]);
        this.incompleteOffsets = longs;
    }

    String makeOffsetMetadataPayload(Set<Long> incompleteOffsets) {
        String offsetMap = serialiseIncompleteOffsetMap(incompleteOffsets);
        return partitionOffsetHighWaterMarks + "," + offsetMap;
    }

    @SneakyThrows
    String serialiseIncompleteOffsetMap(Set<Long> incompleteOffsets) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(baos);
        os.writeObject(incompleteOffsets);
        os.close();
        baos.close();
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    /**
     * Truncate our tracked offsets as a commit was successful, so the low water mark rises, and we dont' need to track
     * as much anymore.
     */
    public void onOffsetCommitSuccess(Map<TopicPartition, OffsetAndMetadata> offsetsToSend) {
        // partitionOffsetHighWaterMarks this will get overwritten in due course
        offsetsToSend.forEach((tp, meta) -> {
            Set<Long> offsets = incompleteOffsets.get(tp);
            long newLowWaterMark = meta.offset();
            for (Long offset : offsets) {
                if(offset < newLowWaterMark){
                    offsets.remove(offset);
                }
            }
        });


        throw new RuntimeException();
    }

    public boolean shouldThrottle() {
        return isOverMax();
    }

    private boolean isOverMax() {
        int remaining = getPartitionWorkRemainingCount();
        boolean loadedEnough = remaining > options.getMaxConcurrency() * loadingFactor;
        boolean overMaxInFlight = remaining > options.getMaxUncommittedMessagesToHandle();
        boolean isOverMax = loadedEnough || overMaxInFlight;
        if (isOverMax) {
            log.debug("loadedEnough {} || overMaxInFlight {}", loadedEnough, overMaxInFlight);
        }
        return isOverMax;
    }

    public int getInFlightCount() {
        return inFlightCount;
    }

    public boolean workIsWaitingToBeCompletedSuccessfully() {
        Collection<NavigableMap<Long, WorkContainer<K, V>>> values = processingShards.values();
        for (NavigableMap<Long, WorkContainer<K, V>> value : values) {
            if (!value.isEmpty())
                return true;
        }
        return false;
    }

    public boolean hasWorkInFlight() {
        return getInFlightCount() != 0;
    }

}
