package io.confluent.parallelconsumer.integrationTests;
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.EnumCartesianProductTestSets;
import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.csid.utils.ProgressTracker;
import io.confluent.csid.utils.TrimListRepresentation;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.ConsumerOffsetCommitter;
import io.confluent.parallelconsumer.internal.OffsetCommitter;
import io.confluent.parallelconsumer.internal.ProducerManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.CartesianProductTest;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase.defaultTimeout;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.*;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.waitAtMost;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Originally created to reproduce bug #25 https://github.com/confluentinc/parallel-consumer/issues/25 which was a known
 * issue with multi-threaded use of the {@link KafkaProducer}.
 * <p>
 * After fixing multi threading issues, using Producer transactions was made optional, and this test grew to uncover
 * several issues with the new implementation of committing offsets through the {@link KafkaConsumer}.
 *
 * @see OffsetCommitter
 * @see ConsumerOffsetCommitter
 * @see ProducerManager
 */
@Tag("transactions")
@Slf4j
class TransactionAndCommitModeTest extends BrokerIntegrationTest {

    int LOW_MAX_POLL_RECORDS_CONFIG = 1;

    int DEFAULT_MAX_POLL_RECORDS_CONFIG = 500;

    int HIGH_MAX_POLL_RECORDS_CONFIG = 10_000;

    // is sensitive to changes in metadata size
    @CartesianProductTest(factory = "enumSets")
    void testDefaultMaxPoll(CommitMode commitMode, ProcessingOrder order) {
        int numMessages = 5000;
        if (order.equals(PARTITION))
            numMessages = 1000; // much slower, do less
        runTest(DEFAULT_MAX_POLL_RECORDS_CONFIG, commitMode, order, numMessages);
    }

    @Test
    void testDefaultMaxPollConsumerSyncSlow() {
        runTest(DEFAULT_MAX_POLL_RECORDS_CONFIG, PERIODIC_CONSUMER_SYNC, UNORDERED);
    }

    static CartesianProductTest.Sets enumSets() {
        return new EnumCartesianProductTestSets()
                .add(CommitMode.class)
                .add(ProcessingOrder.class);
    }

    @RepeatedTest(5)
    void testTransactionalDefaultMaxPoll() {
        runTest(DEFAULT_MAX_POLL_RECORDS_CONFIG, PERIODIC_TRANSACTIONAL_PRODUCER, KEY);
    }

    // is sensitive to changes in metadata size
//    @ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = READ)
    @CartesianProductTest(factory = "enumSets")
    public void testLowMaxPoll(CommitMode commitMode, ProcessingOrder order) {
        int numMessages = 5000;
        if (order.equals(PARTITION))
            numMessages = 1000; // much slower
        runTest(LOW_MAX_POLL_RECORDS_CONFIG, commitMode, order, numMessages);
    }

    @CartesianProductTest(factory = "enumSets")
    public void testHighMaxPollEnum(CommitMode commitMode, ProcessingOrder order) {
        int numMessages = 10000;
        if (order.equals(PARTITION))
            numMessages = 1000; // much slower

        runTest(HIGH_MAX_POLL_RECORDS_CONFIG, commitMode, order, numMessages);
    }

    private void runTest(int maxPoll, CommitMode commitMode, ProcessingOrder order) {
        int expectedMessageCount = 30_000;
        runTest(maxPoll, commitMode, order, expectedMessageCount);
    }

    @SneakyThrows
    private void runTest(int maxPoll, CommitMode commitMode, ProcessingOrder order, int expectedCount) {
        String inputName = setupTopic(this.getClass().getSimpleName() + "-input-" + RandomUtils.nextInt());
        String outputName = setupTopic(this.getClass().getSimpleName() + "-output-" + RandomUtils.nextInt());

        int expectedMessageCount = expectedCount;

        ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, expectedMessageCount);

        // pre-produce messages to input-topic
        List<String> expectedKeys = new ArrayList<>();
        log.info("Producing {} messages before starting test", expectedMessageCount);
        List<Future<RecordMetadata>> sends = new ArrayList<>();
        try (Producer<String, String> kafkaProducer = getKcu().createNewProducer(false)) {
            for (int i = 0; i < expectedMessageCount; i++) {
                String key = "key-" + i;
                Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<>(inputName, key, "value-" + i), (meta, exception) -> {
                    if (exception != null) {
                        log.error("Error sending, ", exception);
                    }
                });
                sends.add(send);
                expectedKeys.add(key);
            }
            log.debug("Finished sending test data");
        }
        // make sure we finish sending before next stage
        log.debug("Waiting for broker acks");
        for (Future<RecordMetadata> send : sends) {
            send.get();
        }
        assertThat(sends).hasSize(expectedMessageCount);

        // run parallel-consumer
        log.debug("Starting test");

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoll);
        KafkaConsumer<String, String> newConsumer = getKcu().createNewConsumer(true, consumerProps);

        // increased PC concurrency - improves test stability and performance.
        int numThreads = 64;
//        int numThreads = 1000;
        var pc = new ParallelEoSStreamProcessor<String, String>(ParallelConsumerOptions.<String, String>builder()
                .ordering(order)
                .consumer(newConsumer)
                .producer(getKcu().createNewProducer(commitMode))
                .commitMode(commitMode)
//                .numberOfThreads(1000)
//                .numberOfThreads(100)
//                .numberOfThreads(2)
                .maxConcurrency(numThreads)
                .build());
        pc.subscribe(of(inputName));

        pc.setTimeBetweenCommits(ofSeconds(1));

        // sanity
        TopicPartition tp = new TopicPartition(inputName, 0);
        Map<TopicPartition, Long> beginOffsets = newConsumer.beginningOffsets(of(tp));
        Map<TopicPartition, Long> endOffsets = newConsumer.endOffsets(of(tp));
        assertThat(endOffsets).containsEntry(tp, ((long) expectedMessageCount));
        assertThat(beginOffsets.get(tp)).isZero();


        List<String> consumedKeys = Collections.synchronizedList(new ArrayList<>());
        List<String> producedKeysAcknowledged = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger producedCount = new AtomicInteger(0);

        pc.pollAndProduce(record -> {
                    log.debug("Polled {}", record.offset());
                    consumedKeys.add(record.key());
                    processedCount.incrementAndGet();
                    return new ProducerRecord<>(outputName, record.key(), "data");
                }, consumeProduceResult -> {
                    log.debug("Produced {}", consumeProduceResult.getOut());
                    producedCount.incrementAndGet();
                    producedKeysAcknowledged.add(consumeProduceResult.getIn().key());
                    bar.step();
                }
        );

        // wait for all pre-produced messages to be processed and produced
        Assertions.useRepresentation(new TrimListRepresentation());

        // todo rounds should be 1? progress should always be made
        int roundsAllowed = 10;
//        roundsAllowed = 200;
//        if (commitMode.equals(CONSUMER_SYNC)) {
//            roundsAllowed = 3; // sync consumer commits can take time // fails
////            roundsAllowed = 5; // sync consumer commits can take time // fails
////            roundsAllowed = 10; // sync consumer commits can take time // fails
////            roundsAllowed = 12; // sync consumer commits can take time // // works with no logging
//        }

        ProgressTracker progressTracker = new ProgressTracker(processedCount, null, defaultTimeout);
        var failureMessage = msg("All keys sent to input-topic should be processed and produced, within time (expected: {} commit: {} order: {} max poll: {})",
                expectedMessageCount, commitMode, order, maxPoll);
        try {
            waitAtMost(defaultTimeout)
                    // dynamic reason support still waiting
                    // https://github.com/awaitility/awaitility/pull/193#issuecomment-873116199
                    // https://github.com/confluentinc/parallel-consumer/issues/199
                    .failFast("PC died, check logs.",
                            () -> pc.isClosedOrFailed()
                                    || producedCount.get() > expectedMessageCount)
//                            () -> {
//                                if (pc.isClosedOrFailed())
//                                    return pc.getFailureCause();
//                                else
//                                    return new TerminalFailureException(msg("Too many messages? processedCount.get() {} > expectedMessageCount {}",
//                                            producedCount.get(), expectedMessageCount)); // needs fail-fast feature in 4.0.4
//                            })
                    .alias(failureMessage)
                    .untilAsserted(() -> {
                        log.trace("Processed-count: {}, Produced-count: {}", processedCount.get(), producedCount.get());
                        int delta = producedCount.get() - processedCount.get();
                        if (delta == numThreads && progressTracker.getRounds().get() > 1) {
                            log.error("Here we go fishy...");
                        }

                        //
                        progressTracker.checkForProgressExceptionally();

                        //
                        SoftAssertions all = new SoftAssertions();
                        all.assertThat(new ArrayList<>(consumedKeys)).as("all expected are consumed").hasSameSizeAs(expectedKeys);
                        all.assertThat(new ArrayList<>(producedKeysAcknowledged)).as("all consumed are produced ok ").hasSameSizeAs(expectedKeys);
                        all.assertAll();
                    });
        } catch (ConditionTimeoutException e) {
            log.debug("Expected keys (size {})", expectedKeys.size());
            log.debug("Consumed keys ack'd (size {})", consumedKeys.size());
            log.debug("Produced keys (size {})", producedKeysAcknowledged.size());
            expectedKeys.removeAll(consumedKeys);
            log.info("Missing keys from consumed: {}", expectedKeys);
            fail(failureMessage + "\n" + e.getMessage());
        }

        pc.closeDrainFirst();

        assertThat(processedCount.get())
                .as("messages processed and produced by parallel-consumer should be equal")
                .isEqualTo(producedCount.get());

        // sanity
        assertThat(expectedMessageCount).isEqualTo(processedCount.get());
        assertThat(producedKeysAcknowledged).hasSameSizeAs(expectedKeys);
        // todo performance: tighten up progress check (<2)
        assertThat(progressTracker.getHighestRoundCountSeen()).isLessThan(40);
        bar.close();
    }

}
