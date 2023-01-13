package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.csid.utils.Range;
import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.FakeRuntimeException;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.GeneralTestUtils.time;
import static io.confluent.csid.utils.Range.range;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.*;
import static java.util.Comparator.comparing;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;
import static org.mockito.Mockito.mock;

/**
 * Mocked out comparative volume tests
 */
@Slf4j
class LargeVolumeInMemoryTests extends ParallelEoSStreamProcessorTestBase {

    @SneakyThrows
    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    void load(CommitMode commitMode) {
        setupClients();
        setupParallelConsumerInstance(ParallelConsumerOptions.builder()
                .ordering(UNORDERED)
                .commitMode(commitMode)
                .build());

        // MockProducer#send() and Mockito are very slow, and very slow (thank you YourKit for profiling this)
        // and seems to get slower the more records are sent, but YourKit won't profile inside #send - I don't know why
        // All quantities do pass the test though.
//        int quantityOfMessagesToProduce = 200_000;
//        int quantityOfMessagesToProduce = 100_000;
        int quantityOfMessagesToProduce = 5_000;
//        int quantityOfMessagesToProduce = 10_000;
//        int quantityOfMessagesToProduce = 500;

        ktu.generateRecords(Optional.of(consumerSpy), quantityOfMessagesToProduce);

        ProgressBar progress = ProgressBarUtils.getNewMessagesBar("Processing", log, quantityOfMessagesToProduce);

        AtomicInteger ackd = new AtomicInteger();
        parallelConsumer.pollAndProduceMany((rec) -> {
            ProducerRecord<String, String> mock = mock(ProducerRecord.class);
            return UniLists.of(mock);
        }, (x) -> {
            progress.stepTo(ackd.incrementAndGet());
        });

        //
        waitAtMost(defaultTimeout.multipliedBy(50))
                .untilAsserted(() -> assertThat(ackd.get()).isEqualTo(quantityOfMessagesToProduce));

        parallelConsumer.close();
        progress.close();


        // assert quantity of produced messages
        List<ProducerRecord<String, String>> history = producerSpy.history();
        assertThat(history).hasSize(quantityOfMessagesToProduce);

        if (commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER)) {
            // assert order of commits
            assertCommitsAlwaysIncrease();

            // producer (tx) commits
            List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> producerCommits = producerSpy.consumerGroupOffsetsHistory();
            assertThat(producerCommits).isNotEmpty();
            long mostRecentProducerCommitOffset = findMostRecentCommitOffset(producerSpy); // tx
            assertThat(mostRecentProducerCommitOffset).isEqualTo(quantityOfMessagesToProduce);
        } else {
            // assert commit messages
            List<Map<TopicPartition, OffsetAndMetadata>> consumerCommitHistory = consumerSpy.getCommitHistoryInt();

            assertThat(consumerCommitHistory).isNotEmpty();
            long mostRecentConsumerCommitOffset = new ArrayList<>(consumerCommitHistory.get(consumerCommitHistory.size() - 1).values()).get(0).offset(); // non-tx
            assertThat(mostRecentConsumerCommitOffset).isEqualTo(quantityOfMessagesToProduce);
        }
    }

    private void assertCommitsAlwaysIncrease() {
        var map = new HashMap<TopicPartition, List<Long>>();
        for (var stringMapMap : producerSpy.consumerGroupOffsetsHistory()) {
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : stringMapMap.get(CONSUMER_GROUP_ID).entrySet()) {
                map.computeIfAbsent(entry.getKey(), (ignore) -> new ArrayList<>()).add(entry.getValue().offset());
            }
        }
        log.trace("Sorted offset commit history: {}", map);
        for (var entry : map.entrySet()) {
            var value = entry.getValue();
            long lastSeenOffset = value.get(0);
            for (var offset : value) {
                if (lastSeenOffset > offset)
                    throw new AssertionError("Offsets not in incrementing order: last seen: " + lastSeenOffset + " vs current: " + offset);
            }
        }
    }

    private long findMostRecentCommitOffset(MockProducer<?, ?> producerSpy) {
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> commitHistory = producerSpy.consumerGroupOffsetsHistory();
        assertThat(commitHistory).as("No offsets committed").hasSizeGreaterThan(0);
        Map<String, Map<TopicPartition, OffsetAndMetadata>> mostRecent = commitHistory.get(commitHistory.size() - 1);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = mostRecent.get(CONSUMER_GROUP_ID);
        OffsetAndMetadata mostRecentTPCommit = topicPartitionOffsetAndMetadataMap.get(new TopicPartition(INPUT_TOPIC, 0));

        return mostRecentTPCommit.offset();
    }

    /**
     * Test comparative performance of different ordering restrictions and different key spaces.
     * <p>
     * Doesn't currently compare different key sizes, only partition order vs key order vs unordered
     */
    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    void timingOfDifferentOrderingTypes(CommitMode commitMode) {
        var quantityOfMessagesToProduce = 10_00;
        var defaultNumKeys = 20;

        ParallelConsumerOptions<?, ?> baseOptions = ParallelConsumerOptions.builder()
                .ordering(UNORDERED)
                .commitMode(commitMode)
                .build();

        setupParallelConsumerInstance(baseOptions);

        Duration unorderedDuration = null;

        // warm up
        var numberOfWarmUpRoundsToPerform = 2;
        for (var round : range(numberOfWarmUpRoundsToPerform)) { // warm up round first
            setupParallelConsumerInstance(baseOptions.toBuilder().ordering(UNORDERED).build());
            log.debug("Using UNORDERED mode");
            unorderedDuration = time(() -> testTiming(defaultNumKeys, quantityOfMessagesToProduce));
            log.info("Duration for Unordered processing in round {} with {} keys was {}", round, defaultNumKeys, unorderedDuration);
        }

        // actual test
        var keyOrderingSizeToResults = new TreeMap<Integer, Duration>();
        for (var keySize : UniLists.of(1, 2, 5, 10, 20, 50, 100, 1_000)) {
            setupParallelConsumerInstance(baseOptions.toBuilder().ordering(KEY).build());
            log.debug("By key, {} keys", keySize);
            var keyOrderDuration = time(() -> testTiming(keySize, quantityOfMessagesToProduce));
            log.info("Duration for Key order processing {} keys was {}", keySize, keyOrderDuration);
            keyOrderingSizeToResults.put(keySize, keyOrderDuration);
        }

        setupParallelConsumerInstance(baseOptions.toBuilder().ordering(PARTITION).build());
        log.debug("By partition");
        var partitionOrderDuration = time(() -> testTiming(defaultNumKeys, quantityOfMessagesToProduce));
        log.info("Duration for Partition order processing {} keys was {}", defaultNumKeys, partitionOrderDuration);

        log.info("Key duration results:\n{}", keyOrderingSizeToResults);

        log.info("Unordered duration: {}", unorderedDuration);

        assertThat(unorderedDuration).as("UNORDERED should be faster than PARTITION order")
                .isLessThan(partitionOrderDuration);

        // compare key order to partition order
        int numOfKeysToCompare = 20; // needs to be small enough that there's a significant difference between unordered and key of x
        Duration keyOrderHalfDefaultKeySize = keyOrderingSizeToResults.get(numOfKeysToCompare);

        // too brittle to have in CI
//        if (commitMode.equals(PERIODIC_CONSUMER_SYNC)) {
//            assertThat(unorderedDuration)
//                    .as("Committing synchronously from the controller causes a large overhead, making UNORDERED " +
//                            "very close in speed to KEY order, keySize of: " + numOfKeysToCompare)
//                    .isCloseTo(keyOrderHalfDefaultKeySize,
//                            keyOrderHalfDefaultKeySize.plus(keyOrderHalfDefaultKeySize.dividedBy(5))); // within 20%
//        } else {
//            assertThat(unorderedDuration)
//                    .as("UNORDERED should be faster than KEY order, keySize of: " + numOfKeysToCompare)
//                    .isLessThan(keyOrderHalfDefaultKeySize);
//        }

        assertThat(keyOrderHalfDefaultKeySize)
                .as("KEY order should be faster than PARTITION order")
                .isLessThan(partitionOrderDuration);
    }

    /**
     * Simple sanity test for the main test function
     */
    @Test
    void testTimingTest() {
        // test that the utility method works for a simple case
        var quantityOfMessagesToProduce = 100;
        var defaultNumKeys = 10;

        testTiming(defaultNumKeys, quantityOfMessagesToProduce);
    }

    /**
     * Runs a round of consumption and returns the time taken
     */
    private void testTiming(int numberOfKeys, int quantityOfMessagesToProduce) {
        log.info("Running test for {} keys and {} messages", numberOfKeys, quantityOfMessagesToProduce);

        List<WorkContainer<String, String>> successfulWork = new ArrayList<>();
        super.injectWorkSuccessListener(parallelConsumer.getWm(), successfulWork);

        List<Integer> keys = Range.listOfIntegers(numberOfKeys);
        ktu.generateRecords(Optional.of(consumerSpy), keys, quantityOfMessagesToProduce);

        ProgressBar producerProgress = ProgressBarUtils.getNewMessagesBar("Production", log, quantityOfMessagesToProduce);

        Queue<ConsumerRecord<String, String>> processingCheck = new ConcurrentLinkedQueue<>();

        var ackRecords = new ConcurrentLinkedQueue<>();
        AtomicInteger count = new AtomicInteger();

        parallelConsumer.pollAndProduceMany((rec) -> {
            count.incrementAndGet();

            processingCheck.add(rec.getSingleConsumerRecord());

            log.debug("Processing: {} count: {}", rec.offset(), count.get());

            ThreadUtils.sleepQuietly(3);
            ProducerRecord<String, String> stub = new ProducerRecord<>(OUTPUT_TOPIC, "sk:" + rec.key(), "SourceV: " + rec.value());
            producerProgress.stepTo(producerSpy.history().size());
            return UniLists.of(stub);
        }, (x) -> {
            // noop
            log.trace("ACK'd {}", x.getOut().value());
            ackRecords.add(x);
        });

        waitAtMost(defaultTimeout.multipliedBy(2)).untilAsserted(() -> {
            assertThat(ackRecords)
                    .as("All expected messages were processed and successful")
                    .hasSize(quantityOfMessagesToProduce);

            assertThat(producerSpy.history())
                    .as("Expected number of produced messages")
                    .hasSize(quantityOfMessagesToProduce);
        });

        //
        producerProgress.close();

        log.info("Closing async client");
        parallelConsumer.close();

        assertCommitsAlwaysIncrease();

        //
        if (processingCheck.size() != quantityOfMessagesToProduce) {
            int stepIndex = 0;
            List<ConsumerRecord<String, String>> processingCheckCollection = Lists.newArrayList(processingCheck.iterator());
            processingCheckCollection.sort(comparing(record -> Integer.parseInt(record.value())));
            log.error("Expectation mismatch - where are my messages?");
            for (ConsumerRecord<String, String> rec : processingCheckCollection) {
                int i = Integer.parseInt(rec.value());
                if (stepIndex != i) {
                    log.error("bad step: {} vs {}", stepIndex, i);
                    throw new FakeRuntimeException("bad process step, expected message is missing: " + stepIndex + " vs " + i);
                }
                stepIndex++;
            }
        }

        // message produced step check
        List<ProducerRecord<String, String>> history = producerSpy.history();
        var missing = new ArrayList<Integer>();
        if (history.size() != quantityOfMessagesToProduce) {
            int stepIndex = 0;
            history.sort(comparing(record -> {
                Objects.requireNonNull(record);
                return Integer.parseInt(record.value());
            }));
            log.error("Expectation mismatch - where are my messages?");
            for (ProducerRecord<String, String> rec : history) {
                int i = Integer.parseInt(rec.value());
                if (stepIndex != i) {
                    log.error("bad step: {} vs {}", stepIndex, i);
                    missing.add(i);
                    stepIndex++;
                }
                stepIndex++;
            }
            if (!missing.isEmpty())
                log.error("Missing: {}", missing);
            throw new FakeRuntimeException("bad step, expected message(s) is missing: " + missing);
        }

        assertThat(producerSpy.history()).as("Finally, all messages expected messages were produced").hasSize(quantityOfMessagesToProduce);
        if (isUsingTransactionalProducer()) {
            List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> groupOffsetsHistory = producerSpy.consumerGroupOffsetsHistory(); // tx
            assertThat(groupOffsetsHistory).as("No offsets committed").hasSizeGreaterThan(0); // tx
        } else {
            List<Map<TopicPartition, OffsetAndMetadata>> commitHistory = consumerSpy.getCommitHistoryInt();
            assertThat(commitHistory).as("No offsets committed").hasSizeGreaterThan(0); // non-tx
        }
    }

}
