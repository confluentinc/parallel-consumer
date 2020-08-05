package io.confluent.csid.utils;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.util.Lists;

import java.util.*;
import java.util.stream.Collectors;

import static io.confluent.csid.asyncconsumer.AsyncConsumerTestBase.CONSUMER_GROUP_ID;
import static io.confluent.csid.asyncconsumer.AsyncConsumerTestBase.INPUT_TOPIC;
import static io.confluent.csid.utils.Range.range;
import static java.lang.Math.random;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

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

    public static void assertCommits(MockProducer mp, List<Integer> integers, Optional<String> description) {
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> maps = mp.consumerGroupOffsetsHistory();
        assertThat(maps).describedAs(description.orElse("Which offsets are committed and in the expected order"))
                .flatExtracting(x ->
                {
                    // get all partition offsets and flatten
                    var results = new ArrayList<Integer>();
                    var y = x.get(CONSUMER_GROUP_ID);
                    for (var z : y.entrySet()) {
                        results.add((int) z.getValue().offset());
                    }
                    return results;
                })
                .isEqualTo(integers);
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
        List<Integer> keyWork = List.copyOf(keys);

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
