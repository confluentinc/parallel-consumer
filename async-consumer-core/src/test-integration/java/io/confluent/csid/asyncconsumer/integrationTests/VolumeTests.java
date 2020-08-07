package io.confluent.csid.asyncconsumer.integrationTests;

import io.confluent.csid.asyncconsumer.AsyncConsumerTestBase;
import io.confluent.csid.utils.KafkaTestUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.util.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.Maps;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import static io.confluent.csid.asyncconsumer.AsyncConsumerOptions.ProcessingOrder.*;
import static io.confluent.csid.utils.GeneralTestUtils.time;
import static io.confluent.csid.utils.Range.range;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Mocked out comparative volume tests
 */
@Slf4j
public class VolumeTests extends AsyncConsumerTestBase {

    KafkaTestUtils ku = new KafkaTestUtils(consumerSpy);

    @SneakyThrows
    @Test
    public void load() {
        setupClients();
        setupAsyncConsumerInstance(UNORDERED);

//        int quantityOfMessagesToProduce = 1_000_000;
        int quantityOfMessagesToProduce = 500;

        List<ConsumerRecord<String, String>> records = ku.generateRecords(quantityOfMessagesToProduce);
        ku.send(consumerSpy, records);

        CountDownLatch latch = new CountDownLatch(quantityOfMessagesToProduce);

        asyncConsumer.asyncPollAndProduce((rec) -> {
            ProducerRecord<String, String> mock = mock(ProducerRecord.class);
            return List.of(mock);
        }, (x) -> {
//            log.debug(x.toString());
            latch.countDown();
        });


        //
//        waitAtMost(defaultTimeout).until(() -> producerSpy.consumerGroupOffsetsHistory().size() > 0);
        latch.await(defaultTimeoutSeconds, SECONDS);
        asyncConsumer.waitForNoInFlight(defaultTimeout.multipliedBy(10));
        asyncConsumer.close();


        // assert quantity of produced messages
        List<ProducerRecord<String, String>> history = producerSpy.history();
        assertThat(history).hasSize(quantityOfMessagesToProduce);

        // assert order of produced messages


        // assert order of commits
//        int keySize = ku.getKeys().size();
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> commitHistory = producerSpy.consumerGroupOffsetsHistory();
        Map<String, Map<TopicPartition, OffsetAndMetadata>> mostRecent = commitHistory.get(commitHistory.size() - 1);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = mostRecent.get(CONSUMER_GROUP_ID);
        OffsetAndMetadata mostRecentTPCommit = topicPartitionOffsetAndMetadataMap.get(new TopicPartition(INPUT_TOPIC, 0));

        assertCommitsAlwaysIncrease();

        assertThat(mostRecentCommit(producerSpy)).isEqualTo(quantityOfMessagesToProduce - 1);

//        TODO assertThat(false).isTrue();
//        Assert process ordering
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

    private long mostRecentCommit(MockProducer<?, ?> producerSpy) {
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> commitHistory = producerSpy.consumerGroupOffsetsHistory();
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
    @Test
    public void timingOfDifferentOrderingTypes() {
        var quantityOfMessagesToProduce = 10_000;
        var defaultNumKeys = 20;

        Duration unorderedDuration = null;
        for (var round : range(2)) { // warm up round first
            setupAsyncConsumerInstance(UNORDERED);
            log.debug("No order");
            unorderedDuration = time(() -> testTiming(defaultNumKeys, quantityOfMessagesToProduce));
            log.info("Duration for Unordered processing in round {} with {} keys was {}", round, defaultNumKeys, unorderedDuration);
        }

        var keyResults = Maps.<Integer, Duration>newTreeMap();
        setupAsyncConsumerInstance(KEY);
        for (var keySize : List.of(1, 2, 5, 10, 20, 50, 100, 1_000, 10_000)) {
            setupAsyncConsumerInstance(KEY);
            log.debug("By key, {} keys", keySize);
            Duration keyOrderDuration = time(() -> testTiming(keySize, quantityOfMessagesToProduce));
            log.info("Duration for Key order processing {} keys was {}", keySize, keyOrderDuration);
            keyResults.put(keySize, keyOrderDuration);
        }

        setupAsyncConsumerInstance(PARTITION);
        log.debug("By partition");
        Duration partitionOrderDuration = time(() -> testTiming(defaultNumKeys, quantityOfMessagesToProduce));
        log.info("Duration for Partition order processing {} keys was {}", defaultNumKeys, partitionOrderDuration);

        log.info("Key duration results:\n{}", keyResults);

        int numOfKeysToCompare = 5; // needs to be small enough that there's a significant difference between unordered and key of x
        Duration keyOrderHalfDefaultKeySize = keyResults.get(numOfKeysToCompare);
        assertThat(unorderedDuration).as("UNORDERED is faster than PARTITION order").isLessThan(partitionOrderDuration);
        assertThat(unorderedDuration).as("UNORDERED is faster than KEY order, keySize of: " + numOfKeysToCompare).isLessThan(keyOrderHalfDefaultKeySize);
        assertThat(keyOrderHalfDefaultKeySize).as("KEY order is faster than PARTITION order").isLessThan(partitionOrderDuration);
    }

    /**
     * Runs a round of consumption and returns the time taken
     */
    private void testTiming(int numberOfKeys, int quantityOfMessagesToProduce) {
        log.info("Running test for {} keys and {} messages", numberOfKeys, quantityOfMessagesToProduce);

        List<Integer> keys = range(numberOfKeys).list();
        HashMap<Integer, List<ConsumerRecord<String, String>>> records = ku.generateRecords(keys, quantityOfMessagesToProduce);
        ku.send(consumerSpy, records);

        ProgressBar bar = new ProgressBarBuilder().setInitialMax(quantityOfMessagesToProduce)
                .showSpeed()
                .setUnit("msgs", 1)
                .setUpdateIntervalMillis(100)
                .build();
        bar.maxHint(quantityOfMessagesToProduce);

        Queue<ConsumerRecord<String, String>> processingCheck = new ConcurrentLinkedQueue<ConsumerRecord<String, String>>();

        asyncConsumer.asyncPollAndProduce((rec) -> {
            processingCheck.add(rec);
            int rangeOfTimeSimulatedProcessingTakesMs = 5;
            long sleepTime = (long) (Math.random() * rangeOfTimeSimulatedProcessingTakesMs);
            sleep(sleepTime);
            ProducerRecord<String, String> stub = new ProducerRecord<>(OUTPUT_TOPIC, "sk:" + rec.key(), "Processing took: " + sleepTime + ". SourceV:" + rec.value());
//            ProducerRecord<String, String> stub = new ProducerRecord<>(OUTPUT_TOPIC, "sk:" + rec.key(), rec.value());
            bar.stepTo(producerSpy.history().size());
            return List.of(stub);
        }, (x) -> {
            // noop
//            log.debug(x.toString());
        });

        Awaitility.waitAtMost(defaultTimeout.multipliedBy(10)).untilAsserted(() -> {
            assertThat(super.successfulWork.size()).as("All messages expected messages were processed and successful").isEqualTo(quantityOfMessagesToProduce);
            assertThat(producerSpy.history().size()).as("All messages expected messages were processed and results produced").isEqualTo(quantityOfMessagesToProduce);
        });
        bar.close();

        log.info("Closing async client");
        asyncConsumer.close();

        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> groupOffsetsHistory = producerSpy.consumerGroupOffsetsHistory();

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
        assertThat(groupOffsetsHistory).as("No offsets committed").hasSizeGreaterThan(0);

        // clear messages
        super.successfulWork.clear();
    }

    @SneakyThrows
    private void sleep(long ms) {
        log.trace("Sleeping for {}", ms);
        Thread.sleep(ms);
    }

}
