package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.EnumCartesianProductTestSets;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.RepeatedTest;
import org.junitpioneer.jupiter.CartesianProductTest;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * Originally created to reproduce bug #25 https://github.com/confluentinc/parallel-consumer/issues/25 which was a known
 * issue with multi threaded use of the {@link org.apache.kafka.clients.producer.KafkaProducer}.
 * <p>
 * After fixing multi threading issue, using Producer transactions was made optional, and this test grew to uncover
 * several issues with the new implementation of committing offsets through the {@link
 * org.apache.kafka.clients.consumer.KafkaConsumer}.
 *
 * @see io.confluent.parallelconsumer.OffsetCommitter
 * @see io.confluent.parallelconsumer.ConsumerOffsetCommitter
 * @see io.confluent.parallelconsumer.ProducerManager
 */
@Slf4j
public class TransactionAndCommitModeTest extends KafkaTest<String, String> {

    int LOW_MAX_POLL_RECORDS_CONFIG = 1;
    int DEFAULT_MAX_POLL_RECORDS_CONFIG = 500;
    int HIGH_MAX_POLL_RECORDS_CONFIG = 10_000;

    public List<String> processedAndProducedKeys = Collections.synchronizedList(new ArrayList<>());
    public AtomicInteger processedCount = new AtomicInteger(0);
    public AtomicInteger producedCount = new AtomicInteger(0);

    // default
    @CartesianProductTest(factory = "enumSets")
    void testDefaultMaxPoll(CommitMode commitMode, ProcessingOrder order) {
        runTest(DEFAULT_MAX_POLL_RECORDS_CONFIG, commitMode, order);
    }

    static CartesianProductTest.Sets enumSets() {
        return new EnumCartesianProductTestSets()
                .add(CommitMode.class)
                .add(ProcessingOrder.class);
    }

    @RepeatedTest(5)
    public void testTransactionalDefaultMaxPoll() {
        runTest(DEFAULT_MAX_POLL_RECORDS_CONFIG, TRANSACTIONAL_PRODUCER, KEY);
    }

    // low
    @CartesianProductTest(factory = "enumSets")
    public void testLowMaxPoll(CommitMode commitMode, ProcessingOrder order) {
        runTest(LOW_MAX_POLL_RECORDS_CONFIG, commitMode, order);
    }

    // high counts
    @CartesianProductTest(factory = "enumSets")
    public void testHighMaxPollEnum(CommitMode commitMode, ProcessingOrder order) {
        runTest(HIGH_MAX_POLL_RECORDS_CONFIG, commitMode, order);
    }

    private void runTest(int maxPoll, CommitMode commitMode) {
        runTest(maxPoll, commitMode, PARTITION);
    }

    @SneakyThrows
    private void runTest(int maxPoll, CommitMode commitMode, ProcessingOrder order) {
        String inputName = setupTopic(this.getClass().getSimpleName() + "-input-" + RandomUtils.nextInt());
        String outputName = setupTopic(this.getClass().getSimpleName() + "-output-" + RandomUtils.nextInt());

        // pre-produce messages to input-topic
        List<String> expectedKeys = new ArrayList<>();
        int expectedMessageCount = 1000;
        log.info("Producing {} messages before starting application", expectedMessageCount);
        try (Producer<String, String> kafkaProducer = kcu.createNewProducer(false)) {
            for (int i = 0; i < expectedMessageCount; i++) {
                String key = "key-" + i;
                kafkaProducer.send(new ProducerRecord<>(inputName, key, "value-" + i));
                expectedKeys.add(key);
            }
        }

        // run parallel-consumer
        log.info("Starting application...");
        KafkaProducer<String, String> newProducer = kcu.createNewProducer(commitMode.equals(TRANSACTIONAL_PRODUCER));

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoll);
        KafkaConsumer<String, String> newConsumer = kcu.createNewConsumer(true, consumerProps);

        var pc = new ParallelEoSStreamProcessor<String, String>(ParallelConsumerOptions.<String, String>builder()
                .ordering(order)
                .consumer(newConsumer)
                .producer(newProducer)
                .commitMode(commitMode)
                .build());
        pc.subscribe(UniLists.of(inputName)); // <5>

        pc.pollAndProduce(record -> {
                    log.trace("Still going {}", record);
                    processedCount.incrementAndGet();
                    return new ProducerRecord<String, String>(outputName, record.key(), "data");
                }, consumeProduceResult -> {
                    producedCount.incrementAndGet();
                    processedAndProducedKeys.add(consumeProduceResult.getIn().key());
                }
        );

        // wait for all pre-produced messages to be processed and produced
        String failureMessage = "All keys sent to input-topic should be processed and produced, within time";
        try {
            waitAtMost(ofSeconds(10)).alias(failureMessage).untilAsserted(() -> {
                log.info("Processed-count: {}, Produced-count: {}", processedCount.get(), producedCount.get());
                List<String> processedAndProducedKeysCopy = new ArrayList<>(processedAndProducedKeys); // avoid concurrent-modification in assert
                assertThat(processedAndProducedKeysCopy).contains(expectedKeys.toArray(new String[0]));
            });
        } catch (ConditionTimeoutException e) {
            log.debug("Expected keys=" + expectedKeys + "");
            log.debug("Processed and produced keys=" + processedAndProducedKeys + "");
            expectedKeys.removeAll(processedAndProducedKeys);
            log.debug("Missing keys=" + expectedKeys);
            fail(failureMessage);
        }

        pc.closeDrainFirst();

        assertThat(processedCount.get())
                .as("messages processed and produced by parallel-consumer should be equal")
                .isEqualTo(producedCount.get());

        // sanity
        assertThat(expectedMessageCount).isEqualTo(processedCount.get());
        assertThat(processedAndProducedKeys).hasSameSizeAs(expectedKeys);
    }

}
