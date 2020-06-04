package io.confluent.csid.asyncconsumer;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import static io.confluent.csid.utils.KafkaUtils.toTP;

@Slf4j
public class InFlightManager<K, V> {

    final private Map<TopicPartition, TreeMap<Long, WorkContainer<K, V>>> inFlightPerPartition = new HashMap<>();

    @Getter(AccessLevel.PACKAGE)
    private final Map<TopicPartition, OffsetAndMetadata> offsetsToSend = new HashMap<>();

    void addWorkToInFlight(WorkContainer<K, V> work, ConsumerRecord<K, V> cr) {
        final TopicPartition inputTopicPartition = work.getTopicPartition();
        long offset = cr.offset();

        // ensure we have a TreeMap (ordered map by key) for the topic partition we're reading from
        var offsetToFuture = inFlightPerPartition
                .computeIfAbsent(inputTopicPartition, (ignore) -> new TreeMap<>());

        // store the future reference, against it's offset as key
        offsetToFuture.put(offset, work);
    }

    @SneakyThrows
    <R> void findCompletedFutureOffsets() {
        int count = 0;
        int removed = 0;
        log.trace("Scanning for in order in-flight work that has completed...");
        for (final var inFlightInPartition : inFlightPerPartition.entrySet()) {
            count += inFlightInPartition.getValue().size();
            var offsetsToRemoveFromInFlight = new LinkedList<Long>();
            TreeMap<Long, WorkContainer<K, V>> inFlightFutures = inFlightInPartition.getValue();
            for (final var offsetAndItsWorkContainer : inFlightFutures.entrySet()) {
                // ordered iteration via offset keys thanks to the tree-map
                WorkContainer<K, V> container = offsetAndItsWorkContainer.getValue();
                boolean complete = container.isComplete();
                if (complete) {
                    long offset = container.getCr().offset();
                    offsetsToRemoveFromInFlight.add(offset);
                    if (container.getUserFunctionSucceeded().get()) {
                        log.trace("Work completed successfully, so marking to commit");
                        foundOffsetToSend(container);
                    } else {
                        log.debug("Offset {} is complete, but failed and is holding up the queue. Ending partition scan.", container.getCr().offset());
                        // can't scan any further
                        break;
                    }
                } else {
                    // can't commit this offset or beyond, as this is the latest offset that is incomplete
                    // i.e. only commit offsets that come before the current one, and stop looking for more
                    log.debug("Offset {} (key:{}) is incomplete, holding up the queue ({}). Ending partition scan.",
                            container.getCr().offset(),
                            container.getCr().key(),
                            inFlightInPartition.getKey());
                    break;
                }
            }

            removed += offsetsToRemoveFromInFlight.size();
            for (Long offset : offsetsToRemoveFromInFlight) {
                inFlightFutures.remove(offset);
            }
        }
        log.debug("Scan finished, {} were in flight, {} completed offsets removed, {} coalesced offset(s) ({}) to be committed",
                count, removed, this.offsetsToSend.size(), offsetsToSend);
    }

    // todo tight loop optimise?
    private void foundOffsetToSend(WorkContainer<K, V> wc) {
        log.trace("Found offset candidate (offset:{}) to add to offset commit map", wc.getCr().offset());
        ConsumerRecord<?, ?> cr = wc.getCr();
        long offset = cr.offset();
        OffsetAndMetadata offsetData = new OffsetAndMetadata(offset, ""); // TODO blank string? move object construction out?
        TopicPartition topicPartitionKey = toTP(cr);
        // as in flights are processed in order, this will keep getting overwritten with the highest offset available
        offsetsToSend.put(topicPartitionKey, offsetData);
    }

    boolean hasInFlightRemaining() {
        for (var entry : inFlightPerPartition.entrySet()) {
            TopicPartition x = entry.getKey();
            TreeMap<Long, WorkContainer<K, V>> workInPartitionInFlight = entry.getValue();
            if (!workInPartitionInFlight.isEmpty())
                return true;
        }
        return false;
    }

    void clearOffsetsToSend() {
        this.offsetsToSend.clear();// = new HashMap<>();// todo remove? smelly?
    }

    boolean hasOffsetsToCommit() {
        return !offsetsToSend.isEmpty();
    }

    boolean hasWorkLeft() {
        return hasInFlightRemaining() || hasOffsetsToCommit();
    }

}
