package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.*;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.internal.StandardComparisonStrategy;
import org.assertj.core.util.IterableUtil;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.StringUtils.msg;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.waitAtMost;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Test running with multiple instances of parallel-consumer consuming from topic with two partitions.
 */
@Slf4j
public class MultiInstanceRebalanceTest extends BrokerIntegrationTest<String, String> {

    static final int DEFAULT_MAX_POLL = 500;
    List<String> consumedKeys = Collections.synchronizedList(new ArrayList<>());
    AtomicInteger count = new AtomicInteger();

    @ParameterizedTest
    @EnumSource(ProcessingOrder.class)
    public void consumeWithMultipleInstancesPeriodicConsumerSync(ProcessingOrder order) {
        runTest(DEFAULT_MAX_POLL, CommitMode.PERIODIC_CONSUMER_SYNC, order);
    }

    @ParameterizedTest
    @EnumSource(ProcessingOrder.class)
    public void consumeWithMultipleInstancesPeriodicConsumerAsync(ProcessingOrder order) {
        runTest(DEFAULT_MAX_POLL, CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS, order);
    }

    @SneakyThrows
    private void runTest(int maxPoll, CommitMode commitMode, ProcessingOrder order) {
        numPartitions = 2;
        String inputName = setupTopic(this.getClass().getSimpleName() + "-input-" + RandomUtils.nextInt());

        // pre-produce messages to input-topic
        List<String> expectedKeys = new ArrayList<>();
        int expectedMessageCount = 100;
        log.info("Producing {} messages before starting test", expectedMessageCount);
        List<Future<RecordMetadata>> sends = new ArrayList<>();
        try (Producer<String, String> kafkaProducer = kcu.createNewProducer(false)) {
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

        ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, expectedMessageCount);

        ExecutorService pcExecutor = Executors.newFixedThreadPool(2);

        // Submit first parallel-consumer
        log.info("Running first instance of pc");
        ParallelConsumerRunnable pc1 = new ParallelConsumerRunnable(maxPoll, commitMode, order, inputName, bar);
        pcExecutor.submit(pc1);

        // Wait for first consumer to consume messages
        Awaitility.waitAtMost(Duration.ofSeconds(10)).until(() -> consumedKeys.size() > 10);

        // Submit second parallel-consumer
        log.info("Running second instance of pc");
        ParallelConsumerRunnable pc2 = new ParallelConsumerRunnable(maxPoll, commitMode, order, inputName, bar);
        pcExecutor.submit(pc2);

        // wait for all pre-produced messages to be processed and produced
        Assertions.useRepresentation(new TrimListRepresentation());
        var failureMessage = msg("All keys sent to input-topic should be processed, within time (expected: {} commit: {} order: {} max poll: {})",
                expectedMessageCount, commitMode, order, maxPoll);
        ProgressTracker progressTracker = new ProgressTracker(count);
        try {
            waitAtMost(ofSeconds(30))
//                    .failFast(() -> pc1.isClosedOrFailed(), () -> pc1.getFailureCause()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
                    .alias(failureMessage)
                    .pollInterval(1, SECONDS)
                    .untilAsserted(() -> {
                        log.trace("Processed-count: {}", consumedKeys.size());
                        if (progressTracker.hasProgressNotBeenMade()) {
                            expectedKeys.removeAll(consumedKeys);
                            throw progressTracker.constructError(msg("No progress, missing keys: {}.", expectedKeys));
                        }
                        SoftAssertions all = new SoftAssertions();
                        all.assertThat(new ArrayList<>(consumedKeys)).as("all expected are consumed").containsAll(expectedKeys);
                        // NB: Re-balance causes re-processing, and this is probably expected. Leaving test like this anyway
                        all.assertThat(new ArrayList<>(consumedKeys)).as("all expected are consumed only once").hasSizeGreaterThanOrEqualTo(expectedKeys.size());

                        all.assertAll();
                    });
        } catch (ConditionTimeoutException e) {
            fail(failureMessage + "\n" + e.getMessage());
        }

        bar.close();

        pc1.getParallelConsumer().closeDrainFirst();
        pc2.getParallelConsumer().closeDrainFirst();

        pcExecutor.shutdown();

        Collection<?> duplicates = IterableUtil.toCollection(StandardComparisonStrategy.instance().duplicatesFrom(consumedKeys));
        log.info("Duplicate consumed keys (at least one is expected due to the rebalance): {}", duplicates);
    }

    int instanceId = 0;

    @Getter
    @RequiredArgsConstructor
    class ParallelConsumerRunnable implements Runnable {

        private final int maxPoll;
        private final CommitMode commitMode;
        private final ProcessingOrder order;
        private final String inputTopic;
        private final ProgressBar bar;
        private ParallelEoSStreamProcessor<String, String> parallelConsumer;

        @Override
        public void run() {
            log.info("Running consumer!");

            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoll);
            KafkaConsumer<String, String> newConsumer = kcu.createNewConsumer(false, consumerProps);

            this.parallelConsumer = new ParallelEoSStreamProcessor<String, String>(ParallelConsumerOptions.<String, String>builder()
                    .ordering(order)
                    .consumer(newConsumer)
                    .commitMode(commitMode)
                    .maxConcurrency(1)
                    .build());
            parallelConsumer.setMyId(Optional.of("id: " + instanceId));
            instanceId++;

            parallelConsumer.subscribe(of(inputTopic));

            parallelConsumer.poll(record -> {
                        // simulate work
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                        count.incrementAndGet();
                        bar.stepBy(1);
                        consumedKeys.add(record.key());
                    }
            );
        }
    }


}
