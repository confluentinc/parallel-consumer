package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;

import java.util.*;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.lang.Math.random;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@RequiredArgsConstructor
public class KafkaTestUtils {

    private final String INPUT_TOPIC;

    private final String CONSUMER_GROUP_ID;

    @Getter
    private final LongPollingMockConsumer consumerSpy;

    int offset = 0;

    // todo not used anymore - delete?
    public void assignConsumerToTopic(final MockConsumer mc) {
        TopicPartition tp1 = new TopicPartition(INPUT_TOPIC, 1);
        TopicPartition tp0 = new TopicPartition(INPUT_TOPIC, 0);
        mc.assign(Lists.list(tp0, tp1));

        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(tp0, 0L);
        beginningOffsets.put(tp1, 0L);
        mc.updateBeginningOffsets(beginningOffsets);
    }

    /**
     * It's a race to see if the genesis offset gets committed or not. So lets remove it if it exists, and all tests can
     * assume it doesn't.
     */
    public static List<Integer> trimAllGenesisOffset(final List<Integer> collect) {
        return collect.stream().filter(x -> x != 0).collect(Collectors.toList());
    }

    public ConsumerRecord<String, String> makeRecord(String key, String value) {
        return makeRecord(0, key, value);
    }

    public ConsumerRecord<String, String> makeRecord(int part, String key, String value) {
        ConsumerRecord<String, String> stringStringConsumerRecord = new ConsumerRecord<>(INPUT_TOPIC, part, offset, (key), (value));
        offset++;
        return stringStringConsumerRecord;
    }

    public void assertCommits(MockProducer mp, List<Integer> integers) {
        assertCommits(mp, integers, Optional.empty());
    }

    /**
     * Collects into a set - ignore repeated commits ({@link OffsetMapCodecManager}).
     * <p>
     * Like {@link AbstractParallelEoSStreamProcessorTestBase#assertCommits(List, Optional)} but for a
     * {@link MockProducer}.
     *
     * @see AbstractParallelEoSStreamProcessorTestBase#assertCommits(List, Optional)
     * @see OffsetMapCodecManager
     */
    public void assertCommits(MockProducer mp, List<Integer> expectedOffsets, Optional<String> description) {
        log.debug("Asserting commits of {}", expectedOffsets);
        List<Integer> offsets = getProducerCommitsFlattened(mp);

        if (!expectedOffsets.contains(0)) {
            offsets = KafkaTestUtils.trimAllGenesisOffset(offsets);
        }

        assertThat(offsets).describedAs(description.orElse("Which offsets are committed and in the expected order"))
                .containsExactlyElementsOf(expectedOffsets);
    }

    public List<Integer> getProducerCommitsFlattened(MockProducer mp) {
        return getProducerCommitsMeta(mp).stream().map(x -> (int) x.offset()).collect(Collectors.toList());
    }

    public List<OffsetAndMetadata> getProducerCommitsMeta(MockProducer mp) {
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> history = mp.consumerGroupOffsetsHistory();

        List<OffsetAndMetadata> set = history.stream().flatMap(histories -> {
            // get all partition offsets and flatten
            ArrayList<OffsetAndMetadata> results = new ArrayList<>();
            var group = histories.get(CONSUMER_GROUP_ID);
            for (var partitionOffsets : group.entrySet()) {
                OffsetAndMetadata commit = partitionOffsets.getValue();
                results.add(commit);
            }
            return results.stream();
        }).collect(Collectors.toList()); // set - ignore repeated commits ({@link OffsetMap})
        return set;
    }

    public void assertCommitLists(MockProducer mp, List<List<Integer>> expectedPartitionOffsets, Optional<String> description) {
        assertCommitLists(mp.consumerGroupOffsetsHistory(), expectedPartitionOffsets, description);
    }

    /**
     * Collects into a set - ignore repeated commits ({@link OffsetMapCodecManager}).
     * <p>
     * Ignores duplicates.
     *
     * @see OffsetMapCodecManager
     */
    public void assertCommitLists(List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> history,
                                  List<List<Integer>> expectedPartitionOffsets,
                                  Optional<String> description) {
        log.info("Asserting commits of {}", expectedPartitionOffsets);

        AtomicReference<String> topicName = new AtomicReference<>("");
        var partitionToCommittedOffsets = new HashMap<TopicPartition, Set<Integer>>(); // set - ignore repeated commits ({@link OffsetMap})
        new ArrayList<>(history).stream().forEachOrdered(histories -> {
            // get all partition offsets and flatten
            var partitionCommits = histories.get(CONSUMER_GROUP_ID);
            Set<Map.Entry<TopicPartition, OffsetAndMetadata>> entries = partitionCommits.entrySet();
            ArrayList<Map.Entry<TopicPartition, OffsetAndMetadata>> wrapped = new ArrayList<>(entries);
            for (var singlePartitionCommit : wrapped) {
                TopicPartition key = singlePartitionCommit.getKey();
                topicName.set(key.topic());
                OffsetAndMetadata commit = singlePartitionCommit.getValue();
                int offset = (int) commit.offset();
                partitionToCommittedOffsets.computeIfAbsent(key, x -> new HashSet<>());
                // ignore genesis commits
                if (offset != 0)
                    partitionToCommittedOffsets.get(key).add(offset);
            }
        });

        // compute the matching expected map
        var expectedMap = new HashMap<TopicPartition, Set<Integer>>();
        for (int i = 0; i < expectedPartitionOffsets.size(); i++) {
            List<Integer> offsets = expectedPartitionOffsets.get(i);
            var tp = new TopicPartition(topicName.get(), i);
            expectedMap.put(tp, new HashSet<>(offsets));
        }

        assertThat(partitionToCommittedOffsets)
                .describedAs(description.orElse("Which offsets are committed and in the expected order"))
                .containsExactlyEntriesOf(expectedMap);
    }

    public static void assertLastCommitIs(final LongPollingMockConsumer<String, String> mockConsumer, final int expected) {
        List<Map<TopicPartition, OffsetAndMetadata>> commits = mockConsumer.getCommitHistoryInt();
        Assertions.assertThat(commits).isNotEmpty();
        long offset = (int) commits.get(commits.size() - 1).values().iterator().next().offset();
        Assertions.assertThat(offset).isEqualTo(expected);
    }

    public List<ConsumerRecord<String, String>> generateRecords(Optional<MockConsumer<String, String>> consumerSpy, int quantity) {
        HashMap<Integer, List<ConsumerRecord<String, String>>> integerListHashMap = generateRecords(consumerSpy, defaultKeys, quantity);
        Collection<List<ConsumerRecord<String, String>>> values = integerListHashMap.values();
        return flatten(values);
    }

    /**
     * @deprecated see {@link #send(MockConsumer, HashMap)}
     */
    @Deprecated
    public List<ConsumerRecord<String, String>> generateRecords(int quantity) {
        return generateRecords(Optional.empty(), quantity);
    }

    /**
     * @deprecated see {@link #send(MockConsumer, HashMap)}
     */
    @Deprecated
    public HashMap<Integer, List<ConsumerRecord<String, String>>> generateRecords(List<Integer> keys, int quantity) {
        return generateRecords(Optional.empty(), keys, quantity);
    }

    /**
     * Randomly create records for randomly selected keys until requested quantity is reached.
     */
    public HashMap<Integer, List<ConsumerRecord<String, String>>> generateRecords(Optional<MockConsumer<String, String>> consumerSpy,
                                                                                  List<Integer> keys, int quantity) {
        var keyRecords = new HashMap<Integer, List<ConsumerRecord<String, String>>>(quantity);
        var newMessagesBar = ProgressBarUtils.getNewMessagesBar("Generating", log, quantity);
        int globalCount = 0;
        while (globalCount < quantity) {
            Integer key = getRandomKey(keys);
            int recsCountForThisKey = (int) (Math.random() * quantity);
            String keyString = key.toString();
            final List<ConsumerRecord<String, String>> keyList = keyRecords.computeIfAbsent(key, (ignore) -> new ArrayList<>());
            int keyCount = keyList.size();
            String value = keyCount + "," + globalCount;
            ConsumerRecord<String, String> rec = makeRecord(keyString, value);
            if (consumerSpy.isPresent()) {
                consumerSpy.get().addRecord(rec);
            }
            keyList.add(rec);
            globalCount++;
            newMessagesBar.step();
        }
        newMessagesBar.close();
        return keyRecords;
    }

    private Integer removeRandomKey(List<Integer> keyWork) {
        int i = (int) (random() * keyWork.size());
        Integer remove = keyWork.remove(i);
        return remove;
    }

    public ArrayList<ConsumerRecord<String, String>> generateRecordsForKey(Integer key, int quantity) {
        var records = new ArrayList<ConsumerRecord<String, String>>(quantity);
        for (long i : Range.range(quantity)) {
            var rec = makeRecord(key.toString(), i + "");
            records.add(rec);
        }
        return records;
    }

    public <KEY, VALUE> List<ConsumerRecord<KEY, VALUE>> flatten(Collection<List<ConsumerRecord<KEY, VALUE>>> listlist) {
        SortedSet<ConsumerRecord<KEY, VALUE>> all = new TreeSet<>(Comparator.comparing(ConsumerRecord::offset));
        for (Collection<ConsumerRecord<KEY, VALUE>> value : listlist) {
            all.addAll(value);
        }
        return new ArrayList<>(all);
    }

    @Setter
    @Getter
    private List<Integer> defaultKeys = Range.listOfIntegers(100);

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

    /**
     * NOTE: Be wary of using this, as ConsumerRecords have their offset hard coded, so it's up to the user to ensure
     * the records are in the order that matches the offset order for the target partition. If the offsets in the
     * virtual partition are not in sequential order, you will see strange test results, where the PC will be ignoring
     * or dropping records as they're not in the correct offset order. Issues may not show up in some tests though.
     *
     * @deprecated records should be added to the MockConsumer as soon as they're created, in order to ensure offset
     *         ordering is correct
     */
    @Deprecated
    public void send(MockConsumer<String, String> consumerSpy, HashMap<?, List<ConsumerRecord<String, String>>> records) {
        for (List<ConsumerRecord<String, String>> value : records.values()) {
            send(consumerSpy, value);
        }
    }

    public void send(MockConsumer<String, String> consumerSpy, List<ConsumerRecord<String, String>> records) {
        log.debug("Sending {} more messages to the consumer stub", records.size());
        // send records in `correct` offset order as declared by the input data, regardless of the order of the input list
        List<ConsumerRecord<String, String>> sorted = new ArrayList<>(records);
        sorted.sort(Comparator.comparingLong(ConsumerRecord::offset));
        var newMessagesBar = ProgressBarUtils.getNewMessagesBar("Sending", log, records.size());
        for (ConsumerRecord<String, String> record : sorted) {
            newMessagesBar.step();
            consumerSpy.addRecord(record);
        }
        newMessagesBar.close();
    }


    public static void completeWork(final WorkManager<String, String> wmm, List<WorkContainer<String, String>> work, long offset) {
        WorkContainer<String, String> workMatchingProvidedOffset = work.stream()
                .filter(x ->
                        x.getCr().offset() == offset
                )
                .findFirst().get();
        KafkaTestUtils.completeWork(wmm, workMatchingProvidedOffset);
    }

    public static void completeWork(final WorkManager<String, String> wmm, final WorkContainer<String, String> wc) {
        FutureTask future = new FutureTask<>(() -> true);
        future.run();
        assertThat(future).isDone();
        wc.setFuture(future);
        wc.onUserFunctionSuccess();
        wmm.onSuccessResult(wc);
        assertThat(wc.isUserFunctionComplete()).isTrue();
    }

    public List<ConsumerRecord<String, String>> sendRecords(final int i) {
        var consumerRecords = generateRecords(i);
        send(consumerSpy, consumerRecords);
        return consumerRecords;
    }

    /**
     * Checks that the ordering of the results is the same as the ordering of the input records
     */
    public static <T> void checkExactOrdering(Map<String, Deque<PollContext<String, String>>> results,
                                              Map<Integer, List<ConsumerRecord<String, String>>> originalRecords) {
        originalRecords.forEach((originalKey, originalRecordList) ->
        {
            var resultSequence = results.get(originalKey.toString());
            isSequenceSequential(originalKey, resultSequence, originalRecordList);
        });
    }

    private static <T> void isSequenceSequential(Integer sequenceKey, Queue<PollContext<String, String>> polledSequence, List<ConsumerRecord<String, String>> originalSequence) {
        assertThat(polledSequence).hasSameSizeAs(originalSequence);
        assertThat(polledSequence)
                .describedAs("Sanity: is same size as original list")
                .hasSameSizeAs(originalSequence);
        log.debug("Key {} has same size of record as original - {}", sequenceKey, polledSequence.size());

        //
        var sortedResult = new ArrayList<>(polledSequence);
        assertThat(sortedResult)
                .as("Results for key %s have same elements and are in the same order as the original records", sequenceKey)
                .extracting(PollContext::value)
                .containsExactlyElementsOf(originalSequence.stream()
                        .map(ConsumerRecord::value).collect(Collectors.toList()));

        // legacy / sanity manual check the integer polledSequence of PollContext value is linear and is without gaps
        var resultIterator = polledSequence.iterator();
        var last = resultIterator.next();
        PollContext<String, String> next = null;
        while (resultIterator.hasNext()) {
            next = resultIterator.next();
            var lastValue = Integer.parseInt(StringUtils.substringBefore(last.value(), ","));
            var thisValue = Integer.parseInt(StringUtils.substringBefore(next.value(), ","));
            var lastValuePlusOne = lastValue + 1;
            boolean isNotLinear = thisValue != lastValuePlusOne;
            if (isNotLinear) {
                log.error("This value {} is not linear with last value {}", thisValue, lastValue);
            }
            assertThat(thisValue).as("polled sequence is sequential").isEqualTo(lastValuePlusOne);
            last = next;
        }
        log.debug("Key {} a an exactly sequential series of values, ending in {} (starts at zero)", sequenceKey, next.value());
    }
}
