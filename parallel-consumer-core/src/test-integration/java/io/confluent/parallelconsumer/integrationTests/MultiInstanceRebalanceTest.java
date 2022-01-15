package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.csid.utils.ProgressTracker;
import io.confluent.csid.utils.TrimListRepresentation;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
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
import org.awaitility.Awaitility;
import org.awaitility.core.TerminalFailureException;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.MDC;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.MDC_INSTANCE_ID;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.IterableUtil.toCollection;
import static org.awaitility.Awaitility.waitAtMost;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Test running with multiple instances of parallel-consumer consuming from topic with two partitions.
 */
//@Isolated // performance sensitive
@Slf4j
class MultiInstanceRebalanceTest extends BrokerIntegrationTest<String, String> {

    static final int DEFAULT_MAX_POLL = 500;
    public static final int CHAOS_FREQUENCY = 500;
    public static final int DEFAULT_POLL_DELAY = 150;
    AtomicInteger count = new AtomicInteger();

    static {
        MDC.put(MDC_INSTANCE_ID, "Test-Thread");
    }

    @ParameterizedTest
    @EnumSource(ProcessingOrder.class)
    void consumeWithMultipleInstancesPeriodicConsumerSync(ProcessingOrder order) {
        numPartitions = 2;
        int expectedMessageCount = (order == PARTITION) ? 100 : 1000;
        int numberOfPcsToRun = 2;
        runTest(DEFAULT_MAX_POLL, CommitMode.PERIODIC_CONSUMER_SYNC, order, expectedMessageCount,
                numberOfPcsToRun, 1.0, DEFAULT_POLL_DELAY);
    }

    @ParameterizedTest
    @EnumSource(ProcessingOrder.class)
    void consumeWithMultipleInstancesPeriodicConsumerAsynchronous(ProcessingOrder order) {
        numPartitions = 2;
        int expectedMessageCount = (order == PARTITION) ? 100 : 1000;
        runTest(DEFAULT_MAX_POLL, CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS, order, expectedMessageCount,
                2, 1.0, DEFAULT_POLL_DELAY);
    }

    /**
     * Tests with very large numbers of parallel consumer instances to try to reproduce state and concurrency issues
     * (#188, #189).
     * <p>
     * This test takes some time, but seems required in order to expose some race conditions without syntehticly
     * creatign them.
     */
    @Disabled
    @Test
    void largeNumberOfInstances() {
        numPartitions = 80;
        int numberOfPcsToRun = 12;
        int expectedMessageCount = 500000;
        runTest(DEFAULT_MAX_POLL, CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS, ProcessingOrder.UNORDERED, expectedMessageCount,
                numberOfPcsToRun, 0.3, 1);
    }

    ProgressBar overallProgress;
    Set<String> overallConsumedKeys = new ConcurrentHashSet<>();

    @SneakyThrows
    private void runTest(int maxPoll, CommitMode commitMode, ProcessingOrder order, int expectedMessageCount,
                         int numberOfPcsToRun, double fractionOfMessagesToPreProduce, int pollDelayMs) {
        String inputName = setupTopic(this.getClass().getSimpleName() + "-input-" + RandomUtils.nextInt());

        overallProgress = ProgressBarUtils.getNewMessagesBar("overall", log, expectedMessageCount);

        ExecutorService pcExecutor = Executors.newWorkStealingPool();

        var sendingProgress = ProgressBarUtils.getNewMessagesBar("sending", log, expectedMessageCount);

        // pre-produce messages to input-topic
        Set<String> expectedKeys = new ConcurrentHashSet<>();
        log.info("Producing {} messages before starting test", expectedMessageCount);
        List<Future<RecordMetadata>> sends = new ArrayList<>();
        int preProduceCount = (int) (expectedMessageCount * fractionOfMessagesToPreProduce);
        try (Producer<String, String> kafkaProducer = kcu.createNewProducer(false)) {
            for (int i = 0; i < preProduceCount; i++) {
                String key = "key-" + i;
                Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<>(inputName, key, "value-" + i), (meta, exception) -> {
                    if (exception != null) {
                        log.error("Error sending, ", exception);
                    }
                    sendingProgress.step();
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
        assertThat(sends).hasSizeGreaterThanOrEqualTo(preProduceCount);

        // Submit first parallel-consumer
        log.info("Running first instance of pc");
        int expectedMessageCountPerPC = expectedMessageCount / numberOfPcsToRun;
        ParallelConsumerRunnable pc1 = new ParallelConsumerRunnable(maxPoll, commitMode, order, inputName, expectedMessageCountPerPC, pollDelayMs);
        pcExecutor.submit(pc1);

        // Wait for first consumer to consume messages, also effectively waits for the group.initial.rebalance.delay.ms (3s by default)
        Awaitility.waitAtMost(ofSeconds(10))
                .until(() -> pc1.getConsumedKeys().size() > 1);

        // keep producing more messages in the background
        var sender = new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                // pre-produce messages to input-topic
                log.info("Producing {} messages before starting test", expectedMessageCount);
                try (Producer<String, String> kafkaProducer = kcu.createNewProducer(false)) {
                    for (int i = preProduceCount; i < expectedMessageCount; i++) {
                        // slow things down just a tad
//                        Thread.sleep(1);
                        String key = "key-" + i;
                        log.debug("sending {}", key);
                        Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<>(inputName, key, "value-" + i), (meta, exception) -> {
                            if (exception != null) {
                                log.error("Error sending, ", exception);
                            }
                            sendingProgress.step();
                        });
                        send.get();
                        sends.add(send);
                        expectedKeys.add(key);
                    }
                    log.info("Finished sending test data");
                }
            }
        };
        pcExecutor.submit(sender);

        // start more PCs
        var secondaryPcs = Collections.synchronizedList(IntStream.range(1, numberOfPcsToRun)
                .mapToObj(value -> {
                            try {
                                int jitterRangeMs = 2;
                                Thread.sleep((int) (Math.random() * jitterRangeMs)); // jitter pc start
                            } catch (InterruptedException e) {
                                log.error(e.getMessage(), e);
                            }
                            log.info("Running pc instance {}", value);
                    ParallelConsumerRunnable instance = new ParallelConsumerRunnable(maxPoll, commitMode, order, inputName, expectedMessageCountPerPC, pollDelayMs);
                            pcExecutor.submit(instance);
                            return instance;
                        }
                ).collect(Collectors.toList()));
        final List<ParallelConsumerRunnable> allPCRunners = Collections.synchronizedList(new ArrayList<>());
        allPCRunners.add(pc1);
        allPCRunners.addAll(secondaryPcs);
        final ParallelConsumerRunnable[] parallelConsumerRunnablesArray = allPCRunners.toArray(new ParallelConsumerRunnable[0]);


        // Randomly start and stop PCs
        var chaosMonkey = new Runnable() {
            @Override
            public void run() {
                try {
                    while (noneHaveFailed(allPCRunners)) {
                        Thread.sleep((int) (CHAOS_FREQUENCY * Math.random()));
                        boolean makeChaos = Math.random() > 0.2; // small chance it will let the test do a run without chaos
//                        boolean makeChaos = true;
                        if (makeChaos) {
                            int size = secondaryPcs.size();
                            int numberToMessWith = (int) (Math.random() * size * 0.6);
                            if (numberToMessWith > 0) {
                                log.info("Will mess with {} instances", numberToMessWith);
                                IntStream.range(0, numberToMessWith).forEach(value -> {
                                    int instanceToGet = (int) ((size - 1) * Math.random());
                                    ParallelConsumerRunnable victim = secondaryPcs.get(instanceToGet);
                                    log.info("Victim is instance: " + victim.instanceId);
                                    victim.toggle(pcExecutor);
                                });
                            }
                        }
                    }
                } catch (Throwable e) {
                    log.error("Error in chaos loop", e);
                    throw new RuntimeException(e);
                }
                log.error("Ending chaos as a PC instance has died");
            }
        };
        pcExecutor.submit(chaosMonkey);


        // wait for all pre-produced messages to be processed and produced
        Assertions.useRepresentation(new TrimListRepresentation());
        var failureMessage = msg("All keys sent to input-topic should be processed, within time (expected: {} commit: {} order: {} max poll: {})",
                expectedMessageCount, commitMode, order, maxPoll);
        ProgressTracker progressTracker = new ProgressTracker(count);
        try {
            waitAtMost(ofMinutes(5))
                    // dynamic reason support still waiting https://github.com/awaitility/awaitility/pull/193#issuecomment-873116199
                    // .failFast( () -> pc1.getFailureCause(), () -> pc1.isClosedOrFailed()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
                    .failFast("A PC has died - check logs", () -> !noneHaveFailed(allPCRunners)) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
                    .alias(failureMessage)
                    .pollInterval(1, SECONDS)
                    .untilAsserted(() -> {
                        log.trace("Processed-count: {}", getAllConsumedKeys(parallelConsumerRunnablesArray).size());
                        if (progressTracker.hasProgressNotBeenMade()) {
                            expectedKeys.removeAll(getAllConsumedKeys(parallelConsumerRunnablesArray));
                            throw progressTracker.constructError(msg("No progress, missing keys: {}.", expectedKeys));
                        }
                        SoftAssertions all = new SoftAssertions();
                        all.assertThat(overallConsumedKeys.containsAll(expectedKeys)).as("contains all: all expected are consumed at least once").isTrue();

                        // is this redundant? containsAll means has size => always true
                        // NB: Re-balance causes re-processing, and this is probably expected. Leaving test like this anyway
                        all.assertThat(overallConsumedKeys).as("size: all expected are consumed only once").hasSizeGreaterThanOrEqualTo(expectedKeys.size());

                        all.assertAll();
                    });
        } catch (Throwable error) {
            List<Exception> exceptions = checkForFailure(allPCRunners);
            if (error instanceof TerminalFailureException) {
                Optional<Exception> any = exceptions.stream().findAny();
                String message = msg("{} \n Terminal failure in one or more of the PCs. Reported exception states are: {} \n {}", failureMessage, exceptions, error);
                throw new RuntimeException(message, any.orElse(null));
            } else {
                String message = msg("{} \n Assertion error. PC reported exception states: {} \n {}", failureMessage, exceptions, error);
                throw new RuntimeException(message, error);
            }
        } finally {
            overallProgress.close();
            sendingProgress.close();
        }

        allPCRunners.forEach(ParallelConsumerRunnable::close);

        assertThat(pc1.consumedKeys).hasSizeGreaterThan(0);
        assertThat(getAllConsumedKeys(secondaryPcs.toArray(new ParallelConsumerRunnable[0])))
                .as("Second PC should have taken over some of the work and consumed some records")
                .hasSizeGreaterThan(0);

        pcExecutor.shutdown();

        Collection<?> duplicates = toCollection(StandardComparisonStrategy.instance()
                .duplicatesFrom(getAllConsumedKeys(parallelConsumerRunnablesArray)));
        log.info("Duplicate consumed keys (at least one is expected due to the rebalance): {}", duplicates);
        double percentageDuplicateTolerance = 0.1;
        assertThat(duplicates)
                .as("There should be few duplicate keys")
                .hasSizeLessThan((int) (expectedMessageCount * percentageDuplicateTolerance)); // in some env, there are a lot more. i.e. Jenkins running parallel suits


    }

    private boolean noneHaveFailed(List<ParallelConsumerRunnable> secondaryPcs) {
        return checkForFailure(secondaryPcs).isEmpty();
    }

    private List<Exception> checkForFailure(List<ParallelConsumerRunnable> secondaryPcs) {
        return secondaryPcs.stream().filter(pcr -> {
            var pc = pcr.getParallelConsumer();
            if (pc == null) return false; // hasn't started
            if (!pc.isClosedOrFailed()) return false; // still open
            boolean failed = pc.getFailureCause() != null; // actually failed
            return failed;
        }).map(pc -> pc.getParallelConsumer().getFailureCause()).collect(Collectors.toList());
    }

    List<String> getAllConsumedKeys(ParallelConsumerRunnable... instances) {
        return Arrays.stream(instances)
                .flatMap(parallelConsumerRunnable -> parallelConsumerRunnable.consumedKeys.stream())
                .collect(Collectors.toList());
    }

    int pcInstanceCount = 0;

    @Getter
    @ToString
    class ParallelConsumerRunnable implements Runnable {

        private final int instanceId;

        private final int maxPoll;
        private final CommitMode commitMode;
        private final ProcessingOrder order;
        private final String inputTopic;
        private final int expectedMessageCount;
        private final ProgressBar bar;
        private final int pollDelayMs;
        private ParallelEoSStreamProcessor<String, String> parallelConsumer;
        private boolean started = false;

        @ToString.Exclude
        private final Queue<String> consumedKeys = new ConcurrentLinkedQueue<>();

        public ParallelConsumerRunnable(int maxPoll, CommitMode commitMode, ProcessingOrder order, String inputTopic, int expectedMessageCount, int pollDelayMs) {
            this.maxPoll = maxPoll;
            this.commitMode = commitMode;
            this.order = order;
            this.inputTopic = inputTopic;
            this.expectedMessageCount = expectedMessageCount;
            this.pollDelayMs = pollDelayMs;

            instanceId = pcInstanceCount;
            pcInstanceCount++;

            bar = ProgressBarUtils.getNewMessagesBar("PC" + instanceId, log, expectedMessageCount);
        }

        @Override
        public void run() {
            MDC.put(MDC_INSTANCE_ID, "Runner-" + instanceId);

            started = true;
            log.info("Running consumer!");

            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoll);
            KafkaConsumer<String, String> newConsumer = kcu.createNewConsumer(false, consumerProps);

            this.parallelConsumer = new ParallelEoSStreamProcessor<>(ParallelConsumerOptions.<String, String>builder()
                    .ordering(order)
                    .consumer(newConsumer)
                    .commitMode(commitMode)
                    .maxConcurrency(10)
                    .build());


            // test was written with 1-second cycles in mind - in terms of expected progression
            this.parallelConsumer.setTimeBetweenCommits(ofSeconds(1));


            parallelConsumer.setMyId(Optional.of("PC-" + instanceId));

            parallelConsumer.subscribe(of(inputTopic));

            parallelConsumer.poll(record -> {
                        // simulate work
                        try {
                            Thread.sleep(pollDelayMs);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                        count.incrementAndGet();
                        this.bar.step();
                        overallProgress.step();
                        consumedKeys.add(record.key());
                        overallConsumedKeys.add(record.key());
                    }
            );
        }

        public void stop() {
            log.info("Stopping {}", this.instanceId);
            started = false;
            parallelConsumer.close();
        }

        public void start(ExecutorService pcExecutor) {
            // strange structure for debugging
            Exception failureCause = getParallelConsumer().getFailureCause();
            if (failureCause != null) {
                throw new RuntimeException("Error starting PC, pc died from previous error: " + failureCause.getMessage(), failureCause);
            }

            log.info("Starting {}", this);
            pcExecutor.submit(this);
        }

        public void close() {
            log.info("Stopping {}", this);
            stop();
            bar.close();
        }

        public void toggle(ExecutorService pcExecutor) {
            if (started) {
                stop();
            } else {
                start(pcExecutor);
            }
        }
    }


}
