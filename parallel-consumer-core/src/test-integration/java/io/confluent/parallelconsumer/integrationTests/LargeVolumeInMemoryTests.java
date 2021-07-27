package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.csid.utils.ThreadUtils;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.shaded.com.google.common.collect.Maps;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.GeneralTestUtils.time;
import static io.confluent.csid.utils.Range.range;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.*;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.SECONDS;
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

//        int quantityOfMessagesToProduce = 1_000_000;
        int quantityOfMessagesToProduce = 500;

        List<ConsumerRecord<String, String>> records = ktu.generateRecords(quantityOfMessagesToProduce);
        ktu.send(consumerSpy, records);

        CountDownLatch allMessagesConsumedLatch = new CountDownLatch(quantityOfMessagesToProduce);

        parallelConsumer.pollAndProduceMany((rec) -> {
            ProducerRecord<String, String> mock = mock(ProducerRecord.class);
            return UniLists.of(mock);
        }, (x) -> {
//            log.debug(x.toString());
            allMessagesConsumedLatch.countDown();
        });

        //
        allMessagesConsumedLatch.await(defaultTimeoutSeconds, SECONDS);
//        waitAtMost(defaultTimeout).until(() -> producerSpy.consumerGroupOffsetsHistory().size() > 0);
        parallelConsumer.waitForProcessedNotCommitted(defaultTimeout.multipliedBy(10));
        parallelConsumer.close();

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
            long mostRecentConsumerCommitOffset = consumerCommitHistory.get(consumerCommitHistory.size() - 1).values().stream().collect(Collectors.toList()).get(0).offset(); // non-tx
            assertThat(mostRecentConsumerCommitOffset).isEqualTo(quantityOfMessagesToProduce);
        }

        // TODO: Assert process ordering
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
        for (var round : range(2)) { // warm up round first
            setupParallelConsumerInstance(baseOptions.toBuilder().ordering(UNORDERED).build());
            log.debug("No order");
            unorderedDuration = time(() -> testTiming(defaultNumKeys, quantityOfMessagesToProduce));
            log.info("Duration for Unordered processing in round {} with {} keys was {}", round, defaultNumKeys, unorderedDuration);
        }

        var keyOrderingSizeToResults = Maps.<Integer, Duration>newTreeMap();
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
     * Runs a round of consumption and returns the time taken
     */
    private void testTiming(int numberOfKeys, int quantityOfMessagesToProduce) {
        log.info("Running test for {} keys and {} messages", numberOfKeys, quantityOfMessagesToProduce);

        List<WorkContainer<String, String>> successfulWork = new Vector<>();
        super.injectWorkSuccessListener(parallelConsumer.getWm(), successfulWork);

        List<Integer> keys = range(numberOfKeys).list();
        HashMap<Integer, List<ConsumerRecord<String, String>>> records = ktu.generateRecords(keys, quantityOfMessagesToProduce);
        ktu.send(consumerSpy, records);

        ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, quantityOfMessagesToProduce);

        Queue<ConsumerRecord<String, String>> processingCheck = new ConcurrentLinkedQueue<ConsumerRecord<String, String>>();

        parallelConsumer.pollAndProduceMany((rec) -> {
            processingCheck.add(rec);
            ThreadUtils.sleepQuietly(3);
            ProducerRecord<String, String> stub = new ProducerRecord<>(OUTPUT_TOPIC, "sk:" + rec.key(), "SourceV: " + rec.value());
            bar.stepTo(producerSpy.history().size());
            return UniLists.of(stub);
        }, (x) -> {
            // noop
//            log.debug(x.toString());
        });

        waitAtMost(defaultTimeout.multipliedBy(15)).untilAsserted(() -> {
            // assertj's size checker uses an iterator so must be synchronised.
            // .size() wouldn't need it but this output is nicer
            synchronized (successfulWork) {
                assertThat(successfulWork)
                        .as("All expected messages were processed and successful")
                        .hasSize(quantityOfMessagesToProduce);
            }

            assertThat(producerSpy.history())
                    .as("Expected number of produced messages")
                    .hasSize(quantityOfMessagesToProduce);
        });
        bar.close();

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
                    throw new RuntimeException("bad process step, expected message is missing: " + stepIndex + " vs " + i);
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
            throw new RuntimeException("bad step, expected message(s) is missing: " + missing);
        }

        assertThat(producerSpy.history().size()).as("Finally, all messages expected messages were produced").isEqualTo(quantityOfMessagesToProduce);
        if (isUsingTransactionalProducer()) {
            List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> groupOffsetsHistory = producerSpy.consumerGroupOffsetsHistory(); // tx
            assertThat(groupOffsetsHistory).as("No offsets committed").hasSizeGreaterThan(0); // tx
        } else {
            List<Map<TopicPartition, OffsetAndMetadata>> commitHistory = consumerSpy.getCommitHistoryInt();
            assertThat(commitHistory).as("No offsets committed").hasSizeGreaterThan(0); // non-tx
        }
    }

}
