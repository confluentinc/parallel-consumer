package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.LongPollingMockConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.Range.range;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.*;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;
import static org.mockito.Mockito.*;
import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
public class ParallelEoSStreamProcessorTestBase {

    public static final String INPUT_TOPIC = "input";
    public static final String OUTPUT_TOPIC = "output";
    public static final String CONSUMER_GROUP_ID = "my-group";

    public static final ConsumerGroupMetadata DEFAULT_GROUP_METADATA = new ConsumerGroupMetadata(CONSUMER_GROUP_ID);

    /**
     * The frequency with which we pretend to poll the broker for records - actually the pretend long poll timeout. A
     * lower value shouldn't affect test speed much unless many different batches of messages are "published".
     *
     * @see LongPollingMockConsumer#poll(Duration)
     */
    public static final int DEFAULT_BROKER_POLL_FREQUENCY_MS = 1000;

    /**
     * The commit interval for the main {@link ParallelEoSStreamProcessor} control thread. Actually the timeout that we
     * poll the {@link LinkedBlockingQueue} for. A lower value will increase the frequency of control loop cycles,
     * making our test waiting go faster.
     *
     * @see ParallelEoSStreamProcessor#workMailBox
     * @see ParallelEoSStreamProcessor#processWorkCompleteMailBox
     */
    public static final int DEFAULT_COMMIT_INTERVAL_MAX_MS = 1000;

    protected LongPollingMockConsumer<String, String> consumerSpy;
    protected MockProducer<String, String> producerSpy;

    protected ParallelEoSStreamProcessor<String, String> parallelConsumer;

    protected static int defaultTimeoutSeconds = 5;

    protected static Duration defaultTimeout = ofSeconds(defaultTimeoutSeconds);
    protected static long defaultTimeoutMs = defaultTimeout.toMillis();
    protected static Duration effectivelyInfiniteTimeout = Duration.ofMinutes(20);

    ParallelEoSStreamProcessorTest.MyAction myRecordProcessingAction;

    ConsumerRecord<String, String> firstRecord;
    ConsumerRecord<String, String> secondRecord;

    KafkaTestUtils ktu;

    protected AtomicReference<Integer> loopCountRef;

    volatile CountDownLatch loopLatchV = new CountDownLatch(0);
    volatile CountDownLatch controlLoopPauseLatch = new CountDownLatch(0);
    protected AtomicReference<Integer> loopCount;

    /**
     * Time to wait to verify some assertion types
     */
    long verificationWaitDelay;

    @BeforeEach
    public void setupAsyncConsumerTestBase() {
        setupParallelConsumerInstance(ParallelConsumerOptions.builder().commitMode(CONSUMER_SYNC).build());
    }

    protected List<WorkContainer<String, String>> successfulWork = Collections.synchronizedList(new ArrayList<>());

    private void setupWorkManager(WorkManager<String, String> wm) {
        wm.getSuccessfulWorkListeners().add((work) -> {
            log.debug("Test work listener heard some successful work: {}", work);
            successfulWork.add(work);
        });
    }

    protected void primeFirstRecord() {
        firstRecord = ktu.makeRecord("key-0", "v0");
        consumerSpy.addRecord(firstRecord);
    }

    protected MockConsumer<String, String> setupClients() {
        instantiateConsumerProducer();

        ktu = new KafkaTestUtils(consumerSpy);

        KafkaTestUtils.assignConsumerToTopic(this.consumerSpy);

        return consumerSpy;
    }

    protected void instantiateConsumerProducer() {
        LongPollingMockConsumer<String, String> consumer = new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockProducer<String, String> producer = new MockProducer<>(true, null, null); // TODO do async testing

        this.producerSpy = spy(producer);
        this.consumerSpy = spy(consumer);
        myRecordProcessingAction = mock(ParallelEoSStreamProcessorTest.MyAction.class);

        when(consumerSpy.groupMetadata()).thenReturn(DEFAULT_GROUP_METADATA);
    }

    /**
     * Need to make sure we only use {@link ParallelEoSStreamProcessor#subscribe} methods, and not do manual assignment,
     * otherwise rebalance listeneres don't fire (because there are never rebalances).
     */
    protected void subscribeParallelConsumerAndMockConsumerTo(String topic) {
        List<String> of = of(topic);
        parallelConsumer.subscribe(of);
        consumerSpy.subscribeWithRebalanceAndAssignment(of, 2);
    }

    protected void setupParallelConsumerInstance(ProcessingOrder order) {
        setupParallelConsumerInstance(ParallelConsumerOptions.builder().ordering(order).build());
    }

    protected void setupParallelConsumerInstance(ParallelConsumerOptions parallelConsumerOptions) {
        setupClients();

        var optionsWithClients = parallelConsumerOptions.toBuilder()
                .consumer(consumerSpy)
                .producer(producerSpy)
                .build();

        parallelConsumer = initAsyncConsumer(optionsWithClients);

        parallelConsumer.setLongPollTimeout(ofMillis(DEFAULT_BROKER_POLL_FREQUENCY_MS));
        parallelConsumer.setTimeBetweenCommits(ofMillis(DEFAULT_COMMIT_INTERVAL_MAX_MS));

        verificationWaitDelay = parallelConsumer.getTimeBetweenCommits().multipliedBy(2).toMillis();

        loopCountRef = attachLoopCounter(parallelConsumer);

        setupWorkManager(parallelConsumer.getWm());
    }

    protected ParallelEoSStreamProcessor<String, String> initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        parallelConsumer = new ParallelEoSStreamProcessor<>(parallelConsumerOptions);

        return parallelConsumer;
    }

    protected void sendSecondRecord(MockConsumer<String, String> consumer) {
        secondRecord = ktu.makeRecord("key-0", "v1");
        consumer.addRecord(secondRecord);
    }

    protected AtomicReference<Integer> attachLoopCounter(ParallelEoSStreamProcessor parallelConsumer) {
        final AtomicReference<Integer> currentLoop = new AtomicReference<>(0);
        parallelConsumer.addLoopEndCallBack(() -> {
            Integer currentNumber = currentLoop.get();
            int newLoopNumber = currentNumber + 1;
            currentLoop.compareAndSet(currentNumber, newLoopNumber);
            log.trace("Counting down latch from {}", loopLatchV.getCount());
            loopLatchV.countDown();
            log.trace("Loop latch remaining: {}", loopLatchV.getCount());
            if (controlLoopPauseLatch.getCount() > 0) {
                log.debug("Waiting on pause latch ({})...", controlLoopPauseLatch.getCount());
                try {
                    controlLoopPauseLatch.await();
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
                log.trace("Completed waiting on pause latch");
            }
            log.trace("Loop count {}", currentLoop.get());
        });
        return currentLoop;
    }

    /**
     * Pauses the control loop by awaiting this injected countdown lunch
     */
    protected void pauseControlLoop() {
        log.trace("Pause loop");
        controlLoopPauseLatch = new CountDownLatch(1);
    }

    /**
     * Resume is the controller by decrementing the injected countdown latch
     */
    protected void resumeControlLoop() {
        log.trace("Resume loop");
        controlLoopPauseLatch.countDown();
    }

    protected void waitForOneLoopCycle() {
        waitForSomeLoopCycles(1);
    }

    protected void waitForSomeLoopCycles(int thisManyMore) {
        log.debug("Waiting for {} more iterations of the control loop.", thisManyMore);
        blockingLoopLatchTrigger(thisManyMore);
        log.debug("Completed waiting on {} loop(s)", thisManyMore);
    }

    protected void waitUntilTrue(Callable<Boolean> booleanCallable) {
        waitAtMost(defaultTimeout).until(booleanCallable);
    }

    /**
     * Make sure the latch is attached, if this times out unexpectedly
     */
    @SneakyThrows
    private void blockingLoopLatchTrigger(int waitForCount) {
        log.debug("Waiting on {} cycles on loop latch...", waitForCount);
        loopLatchV = new CountDownLatch(waitForCount);
        boolean timeout = !loopLatchV.await(defaultTimeoutSeconds, SECONDS);
        if (timeout)
            throw new TimeoutException(msg("Timeout {} waiting for latch", defaultTimeoutSeconds));
    }

    @SneakyThrows
    private void waitForLoopCount(int waitForCount) {
        log.debug("Waiting on {} cycles on loop latch...", waitForCount);
        waitAtMost(defaultTimeout.multipliedBy(100)).until(() -> loopCount.get() > waitForCount);
    }

    protected void waitForCommitExact(int partition, int offset) {
        log.debug("Waiting for commit offset {} on partition {}", offset, partition);
        var expectedOffset = new OffsetAndMetadata(offset, "");
        TopicPartition partitionNumber = new TopicPartition(INPUT_TOPIC, partition);
        var expectedOffsetMap = UniMaps.of(partitionNumber, expectedOffset);
        verify(producerSpy, timeout(defaultTimeoutMs).times(1)).sendOffsetsToTransaction(argThat(
                (offsetMap) -> offsetMap.equals(expectedOffsetMap)),
                any(ConsumerGroupMetadata.class));
    }

    public void assertCommits(List<Integer> offsets, String description) {
        assertCommits(offsets, Optional.of(description));
    }

    /**
     * Flattens the offsets of all partitions into a single sequential list
     */
    public void assertCommits(List<Integer> offsets, Optional<String> description) {
        if (isUsingTransactionalProducer()) {
            KafkaTestUtils.assertCommits(producerSpy, offsets, description);
            assertThat(extractAllPartitionsOffsetsSequentially()).isEmpty();
        } else {
            List<Integer> collect = extractAllPartitionsOffsetsSequentially();
            // duplicates are ok
            // is there a nicer optional way?
            // {@link Optional#ifPresentOrElse} only @since 9
            if (description.isPresent()) {
                assertThat(collect).as(description.get()).hasSameElementsAs(offsets);
            } else {
                assertThat(collect).hasSameElementsAs(offsets);
            }

            KafkaTestUtils.assertCommits(producerSpy, UniLists.of(), Optional.of("Empty"));
        }
    }

    /**
     * Flattens the offsets of all partitions into a single sequential list
     */
    private List<Integer> extractAllPartitionsOffsetsSequentially() {
        var result = new ArrayList<Integer>();
        // copy the list for safe concurrent access
        List<Map<TopicPartition, OffsetAndMetadata>> history = new ArrayList<>(consumerSpy.getCommitHistoryInt());
        return history.stream()
                .flatMap(commits ->
                        commits.values().stream().map(meta -> (int) meta.offset())
                ).collect(Collectors.toList());
    }

    public void assertCommits(List<Integer> offsets) {
        assertCommits(offsets, Optional.empty());
    }

    /**
     * Checks a list of commits of a list of partitions
     */
    public void assertCommitLists(List<List<Integer>> offsets) {
        if (isUsingTransactionalProducer()) {
            KafkaTestUtils.assertCommitLists(producerSpy, offsets, Optional.empty());
        } else {
            KafkaTestUtils.assertCommitLists(consumerSpy.getCommitHistoryWithGropuId(), offsets, Optional.empty());
        }
    }

    protected List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> getCommitHistory() {
        if (isUsingTransactionalProducer()) {
            return producerSpy.consumerGroupOffsetsHistory();
        } else {
            return consumerSpy.getCommitHistoryWithGropuId();
        }
    }

    protected boolean isUsingTransactionalProducer() {
        ParallelConsumerOptions.CommitMode commitMode = parallelConsumer.getWm().getOptions().getCommitMode();
        return commitMode.equals(TRANSACTIONAL_PRODUCER);
    }

    protected boolean isUsingAsyncCommits() {
        ParallelConsumerOptions.CommitMode commitMode = parallelConsumer.getWm().getOptions().getCommitMode();
        return commitMode.equals(CONSUMER_ASYNCHRONOUS);
    }

    protected void awaitLatch(List<CountDownLatch> latches, int latchIndex) {
        log.trace("Waiting on latch {}", latchIndex);
        awaitLatch(latches.get(latchIndex));
    }

    @SneakyThrows
    protected void awaitLatch(CountDownLatch latch) {
        log.trace("Waiting on latch with timeout {}", defaultTimeout);
        boolean latchReachedZero = latch.await(defaultTimeoutSeconds, SECONDS);
        if (latchReachedZero) {
            log.trace("Latch released");
        } else {
            throw new AssertionError("Latch await timeout - " + latch.getCount() + " remaining");
        }
    }

    protected void releaseAndWait(List<CountDownLatch> locks, List<Integer> lockIndexes) {
        for (Integer i : lockIndexes) {
            log.debug("Releasing {}...", i);
            locks.get(i).countDown();
        }
        waitForSomeLoopCycles(1);
    }

    protected void release(List<CountDownLatch> locks, int lockIndex) {
        log.debug("Releasing {}...", lockIndex);
        locks.get(lockIndex).countDown();
    }

    protected void releaseAndWait(List<CountDownLatch> locks, int lockIndex) {
        log.debug("Releasing {}...", lockIndex);
        locks.get(lockIndex).countDown();
        waitForSomeLoopCycles(1);
    }

    protected List<CountDownLatch> constructLatches(int numberOfLatches) {
        var result = new ArrayList<CountDownLatch>(numberOfLatches);
        for (var ignore : range(numberOfLatches)) {
            result.add(new CountDownLatch(1));
        }
        return result;
    }

    protected void pauseControlToAwaitForLatch(CountDownLatch latch) {
        pauseControlLoop();
        awaitLatch(latch);
        resumeControlLoop();
        waitForOneLoopCycle();
    }

}
