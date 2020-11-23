package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.integrationTests.KafkaTest;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.TRANSACTIONAL_PRODUCER;
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
    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    public void testDefaultMaxPoll(CommitMode commitMode) {
        runTest(DEFAULT_MAX_POLL_RECORDS_CONFIG, commitMode);
    }

    @RepeatedTest(5)
    public void testTransactionalDefaultMaxPoll() {
        runTest(DEFAULT_MAX_POLL_RECORDS_CONFIG, TRANSACTIONAL_PRODUCER);
    }

    // low
    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    public void testLowMaxPoll(CommitMode commitMode) {
        runTest(LOW_MAX_POLL_RECORDS_CONFIG, commitMode);
    }

    // high counts
    @ParameterizedTest()
    @EnumSource(CommitMode.class)
    public void testHighMaxPollEnum(CommitMode commitMode) {
        runTest(HIGH_MAX_POLL_RECORDS_CONFIG, commitMode);
    }

    private void runTest(int maxPoll, CommitMode commitMode) {
        boolean tx = commitMode.equals(TRANSACTIONAL_PRODUCER);
        runTest(tx, maxPoll, commitMode);
    }

    @SneakyThrows
    private void runTest(boolean tx, int maxPoll, CommitMode commitMode) {
        String inputName = setupTopic(this.getClass().getSimpleName() + "-input");
        String outputName = setupTopic(this.getClass().getSimpleName() + "-input");

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
        ParallelEoSStreamProcessor<String, String> pc = new ParallelEoSStreamProcessor<>(kcu.consumer, kcu.producer, ParallelConsumerOptions.builder().build());
        pc.subscribe(UniLists.of(inputName)); // <5>

        pc.pollAndProduce(record -> {
                    processedCount.incrementAndGet();
                    return UniLists.of(new ProducerRecord<>(outputName, record.key(), "data"));
                }, consumeProduceResult -> {
                    producedCount.incrementAndGet();
                    processedAndProducedKeys.add(consumeProduceResult.getIn().key());
                }
        );

        // wait for all pre-produced messages to be processed and produced
        String failureMessage = "All keys sent to input-topic should be processed and produced";
        try {
            waitAtMost(ofSeconds(10)).alias(failureMessage).untilAsserted(() -> {
                log.debug("Processed-count: " + processedCount.get());
                log.debug("Produced-count: " + producedCount.get());
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

        pc.close();
    }

}
