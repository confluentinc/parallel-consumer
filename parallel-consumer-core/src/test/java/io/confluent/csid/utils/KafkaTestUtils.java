package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.OffsetMapCodecManager;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.util.Lists;
import pl.tlinkowski.unij.api.UniLists;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.ParallelConsumerTestBase.CONSUMER_GROUP_ID;
import static io.confluent.parallelconsumer.ParallelConsumerTestBase.INPUT_TOPIC;
import static io.confluent.csid.utils.Range.range;
import static java.lang.Math.random;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@Slf4j
@RequiredArgsConstructor
public class KafkaTestUtils {

    final MockConsumer consumerSpy;

    int offset = 0;

    static public final ConsumerGroupMetadata DEFAULT_GROUP_METADATA = new ConsumerGroupMetadata(CONSUMER_GROUP_ID);

    public static void setupConsumer(MockConsumer mc) {
        when(mc.groupMetadata()).thenReturn(DEFAULT_GROUP_METADATA);

        TopicPartition tp1 = new TopicPartition(INPUT_TOPIC, 1);
        TopicPartition tp0 = new TopicPartition(INPUT_TOPIC, 0);
        mc.assign(Lists.list(tp0, tp1));

        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(tp0, 0L);
        beginningOffsets.put(tp1, 0L);
        mc.updateBeginningOffsets(beginningOffsets);
    }

    public ConsumerRecord<String, String> makeRecord(String key, String value) {
        return makeRecord(0, key, value);
    }

    public ConsumerRecord<String, String> makeRecord(int part, String key, String value) {
        ConsumerRecord<String, String> stringStringConsumerRecord = new ConsumerRecord<>(INPUT_TOPIC, part, offset, (key), (value));
        offset++;
        return stringStringConsumerRecord;
    }

    public static void assertCommits(MockProducer mp, List<Integer> integers) {
        assertCommits(mp, integers, Optional.empty());
    }

    /**
     * Collects into a set - ignore repeated commits ({@link OffsetMapCodecManager})
     *
     * @see OffsetMapCodecManager
     */
    public static void assertCommits(MockProducer mp, List<Integer> expectedOffsets, Optional<String> description) {
        log.debug("Asserting commits of {}", expectedOffsets);
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> history = mp.consumerGroupOffsetsHistory();

        Set<Integer> set = history.stream().flatMap(histories -> {
            // get all partition offsets and flatten
            var results = new ArrayList<Integer>();
            var group = histories.get(CONSUMER_GROUP_ID);
            for (var partitionOffsets : group.entrySet()) {
                OffsetAndMetadata commit = partitionOffsets.getValue();
                int offset = (int) commit.offset();
                results.add(offset);
            }
            return results.stream();
        }).collect(Collectors.toSet()); // set - ignore repeated commits ({@link OffsetMap})

        assertThat(set).describedAs(description.orElse("Which offsets are committed and in the expected order"))
                .containsExactlyElementsOf(expectedOffsets);
    }

    /**
     * Collects into a set - ignore repeated commits ({@link OffsetMapCodecManager})
     *
     * @see OffsetMapCodecManager
     */
    public static void assertCommitLists(MockProducer mp, List<List<Integer>> expectedPartitionOffsets, Optional<String> description) {
        log.info("Asserting commits of {}", expectedPartitionOffsets);
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> history = mp.consumerGroupOffsetsHistory();

        AtomicReference<String> topicName = new AtomicReference<>("");
        var results = new HashMap<TopicPartition, Set<Integer>>(); // set - ignore repeated commits ({@link OffsetMap})
        history.stream().forEachOrdered(histories -> {
            // get all partition offsets and flatten
            var group = histories.get(CONSUMER_GROUP_ID);
            for (var actualPartitionOffsets : group.entrySet()) {
                TopicPartition key = actualPartitionOffsets.getKey();
                topicName.set(key.topic());
                OffsetAndMetadata commit = actualPartitionOffsets.getValue();
                int offset = (int) commit.offset();
                results.computeIfAbsent(key, x -> new HashSet<>()).add(offset);
            }
        });

        var expectedMap = new HashMap<TopicPartition, Set<Integer>>();
        for (int i = 0; i < expectedPartitionOffsets.size(); i++) {
            List<Integer> offsets = expectedPartitionOffsets.get(i);
            var tp = new TopicPartition(topicName.get(), i);
            expectedMap.put(tp, new HashSet<>(offsets));
        }

        assertThat(results).describedAs(description.orElse("Which offsets are committed and in the expected order"))
                .containsExactlyEntriesOf(expectedMap);
    }

    public List<ConsumerRecord<String, String>> generateRecords(int quantity) {
        HashMap<Integer, List<ConsumerRecord<String, String>>> integerListHashMap = generateRecords(defaultKeys, quantity);
        return flatten(integerListHashMap.values());
    }

    /**
     * Randomly create records for randomly selected keys until requested quantity is reached.
     */
    public HashMap<Integer, List<ConsumerRecord<String, String>>> generateRecords(List<Integer> keys, int quantity) {
        var keyRecords = new HashMap<Integer, List<ConsumerRecord<String, String>>>(quantity);
        List<Integer> keyWork = UniLists.copyOf(keys);

        int count = 0;
        while (count < quantity) {
            Integer key = getRandomKey(keys);
            int recsCountForThisKey = (int) (Math.random() * quantity);
            ConsumerRecord<String, String> rec = makeRecord(key.toString(), count + "");
//            var consumerRecords = generateRecordsForKey(key, recsCountForThisKey);
            keyRecords.computeIfAbsent(key, (ignore) -> new ArrayList<>()).add(rec);
//            keyRecords.put(key, consumerRecords);
            count++;
        }
        return keyRecords;
    }

    private Integer removeRandomKey(List<Integer> keyWork) {
        int i = (int) (random() * keyWork.size());
        Integer remove = keyWork.remove(i);
        return remove;
    }

    public ArrayList<ConsumerRecord<String, String>> generateRecordsForKey(Integer key, int quantity) {
        var records = new ArrayList<ConsumerRecord<String, String>>(quantity);
        for (int i : range(quantity)) {
            var rec = makeRecord(key.toString(), i + "");
            records.add(rec);
        }
        return records;
    }

    public <T> List<T> flatten(Collection<List<T>> listlist) {
        List<T> all = new ArrayList<>();
        for (Collection<T> value : listlist) {
            all.addAll(value);
        }
        return all;
    }

    @Setter
    @Getter
    private List<Integer> defaultKeys = range(100).list();

    private Integer getRandomDefaultKey() {
        int i = (int) (random() * defaultKeys.size());
        return defaultKeys.get(i);
    }

    private Integer getRandomKey(List<Integer> keyList) {
        int i = (int) (random() * keyList.size());
        return keyList.get(i);
    }

    private ConsumerRecord<String, String> addRecord(String k, String v) {
        ConsumerRecord<String, String> record = makeRecord(k, v);
        consumerSpy.addRecord(record);
        return record;
    }

    public void send(MockConsumer<String, String> consumerSpy, HashMap<?, List<ConsumerRecord<String, String>>> records) {
        List<ConsumerRecord<String, String>> collect = records.entrySet().stream()
                .flatMap(x -> x.getValue().stream())
                .collect(Collectors.toList());
        send(consumerSpy, collect);
    }

    public void send(MockConsumer<String, String> consumerSpy, List<ConsumerRecord<String, String>> records) {
        // send records in `correct` offset order as declared by the input data, regardless of the order of the input list
        List<ConsumerRecord<String, String>> sorted = new ArrayList(records);
        Collections.sort(sorted, Comparator.comparingLong(ConsumerRecord::offset));
        for (ConsumerRecord<String, String> record : sorted) {
            consumerSpy.addRecord(record);
        }
    }
}
