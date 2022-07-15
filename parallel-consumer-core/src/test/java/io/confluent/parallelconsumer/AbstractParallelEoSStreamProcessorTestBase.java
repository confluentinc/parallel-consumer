package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.LongPollingMockConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.model.CommitHistory;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import io.confluent.parallelconsumer.truth.CommitHistorySubject;
import io.confluent.parallelconsumer.truth.LongPollingMockConsumerSubject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.LatchTestUtils.awaitLatch;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.*;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.waitAtMost;
import static org.mockito.Mockito.*;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * @author Antony Stubbs
 * @see AbstractParallelEoSStreamProcessor
 */
// todo migrate commit assertion methods in to a Truth Subject
@Slf4j
public abstract class AbstractParallelEoSStreamProcessorTestBase {

    public String INPUT_TOPIC;
    public String OUTPUT_TOPIC;
    public String CONSUMER_GROUP_ID;

    public ConsumerGroupMetadata DEFAULT_GROUP_METADATA;

    /**
     * The frequency with which we pretend to poll the broker for records - actually the pretend long poll timeout. A
     * lower value shouldn't affect test speed much unless many batches of messages are "published" as test messages are
     * queued up at the beginning and the polled.
     *
     * @see LongPollingMockConsumer#poll(Duration)
     */
    public static final int DEFAULT_BROKER_POLL_FREQUENCY_MS = 500;

    /**
     * The commit interval for the main {@link AbstractParallelEoSStreamProcessor} control thread. Actually the timeout
     * that we poll the {@link LinkedBlockingQueue} for. A lower value will increase the frequency of control loop
     * cycles, making our test waiting go faster.
     *
     * @see AbstractParallelEoSStreamProcessor#workMailBox
     * @see AbstractParallelEoSStreamProcessor#processWorkCompleteMailBox
     */
    public static final int DEFAULT_COMMIT_INTERVAL_MAX_MS = 100;

    protected LongPollingMockConsumer<String, String> consumerSpy;
    protected MockProducer<String, String> producerSpy;

    protected AbstractParallelEoSStreamProcessor<String, String> parentParallelConsumer;

    public static int defaultTimeoutSeconds = 30;

    public static Duration defaultTimeout = ofSeconds(defaultTimeoutSeconds);
    protected static long defaultTimeoutMs = defaultTimeout.toMillis();
    protected static Duration effectivelyInfiniteTimeout = Duration.ofMinutes(20);

    ParallelEoSStreamProcessorTest.MyAction myRecordProcessingAction;

    ConsumerRecord<String, String> firstRecord;
    ConsumerRecord<String, String> secondRecord;

    protected KafkaTestUtils ktu;

    protected AtomicReference<Integer> loopCountRef;

    volatile CountDownLatch loopLatchV = new CountDownLatch(0);
    volatile CountDownLatch controlLoopPauseLatch = new CountDownLatch(0);
    protected AtomicReference<Integer> loopCount;

    /**
     * Time to wait to verify some assertion types
     */
    long verificationWaitDelay;
    protected TopicPartition topicPartition;

    /**
     * Unique topic names for each test method
     */
    public void setupTopicNames() {
        INPUT_TOPIC = "input-" + Math.random();
        OUTPUT_TOPIC = "output-" + Math.random();
        CONSUMER_GROUP_ID = "my-group" + Math.random();
        topicPartition = new TopicPartition(INPUT_TOPIC, 0);
        DEFAULT_GROUP_METADATA = new ConsumerGroupMetadata(CONSUMER_GROUP_ID);
    }

    @BeforeEach
    public void setupAsyncConsumerTestBase() {
        setupTopicNames();

        ParallelConsumerOptions<Object, Object> options = getOptions();
        setupParallelConsumerInstance(options);
    }

    protected ParallelConsumerOptions<Object, Object> getOptions() {
        ParallelConsumerOptions<Object, Object> options = getDefaultOptions()
                .build();
        return options;
    }

    protected ParallelConsumerOptions.ParallelConsumerOptionsBuilder<Object, Object> getDefaultOptions() {
        return ParallelConsumerOptions.builder()
                .commitMode(PERIODIC_CONSUMER_SYNC)
                .ordering(UNORDERED);
    }

    @AfterEach
    public void close() {
        // don't try to close if error'd (at least one test purposefully creates an error to tests error handling) - we
        // don't want to bubble up an error here that we expect from here.
        if (!parentParallelConsumer.isClosedOrFailed()) {
            if (parentParallelConsumer.getFailureCause() != null) {
                log.error("PC has error - test failed");
            }
            log.debug("Test ended (maybe a failure), closing pc...");
            parentParallelConsumer.close();
        } else {
            log.debug("Test finished, pc already closed.");
        }
    }

    protected void injectWorkSuccessListener(WorkManager<String, String> wm, List<WorkContainer<String, String>> customSuccessfulWork) {
        wm.getSuccessfulWorkListeners().add((work) -> {
            log.debug("Test work listener heard some successful work: {}", work);
            synchronized (customSuccessfulWork) {
                customSuccessfulWork.add(work);
            }
        });
    }

    protected void primeFirstRecord() {
        firstRecord = ktu.makeRecord("key-0", "v0-first-primed-record");
        consumerSpy.addRecord(firstRecord);
    }

    protected MockConsumer<String, String> setupClients() {
        instantiateConsumerProducer();

        ktu = new KafkaTestUtils(INPUT_TOPIC, CONSUMER_GROUP_ID, consumerSpy);

        return consumerSpy;
    }

    protected void instantiateConsumerProducer() {
        LongPollingMockConsumer<String, String> consumer = new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockProducer<String, String> producer = new MockProducer<>(true,
                Serdes.String().serializer(), Serdes.String().serializer());

        this.producerSpy = spy(producer);
        this.consumerSpy = spy(consumer);
        myRecordProcessingAction = mock(ParallelEoSStreamProcessorTest.MyAction.class);

        when(consumerSpy.groupMetadata()).thenReturn(DEFAULT_GROUP_METADATA);
    }

    /**
     * Need to make sure we only use {@link AbstractParallelEoSStreamProcessor#subscribe} methods, and not do manual
     * assignment, otherwise rebalance listeners don't fire (because there are never rebalances).
     */
    protected void subscribeParallelConsumerAndMockConsumerTo(String topic) {
        List<String> of = of(topic);
        parentParallelConsumer.subscribe(of);
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

        parentParallelConsumer = initAsyncConsumer(optionsWithClients);

        subscribeParallelConsumerAndMockConsumerTo(INPUT_TOPIC);

        parentParallelConsumer.setLongPollTimeout(ofMillis(DEFAULT_BROKER_POLL_FREQUENCY_MS));
        parentParallelConsumer.setTimeBetweenCommits(ofMillis(DEFAULT_COMMIT_INTERVAL_MAX_MS));

        verificationWaitDelay = parentParallelConsumer.getTimeBetweenCommits().multipliedBy(2).toMillis();

        loopCountRef = attachLoopCounter(parentParallelConsumer);
    }

    protected abstract AbstractParallelEoSStreamProcessor<String, String> initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions);

    protected void sendSecondRecord(MockConsumer<String, String> consumer) {
        secondRecord = ktu.makeRecord("key-0", "v1");
        consumer.addRecord(secondRecord);
    }

    protected AtomicReference<Integer> attachLoopCounter(AbstractParallelEoSStreamProcessor parallelConsumer) {
        final AtomicReference<Integer> currentLoop = new AtomicReference<>(0);
        parentParallelConsumer.addLoopEndCallBack(() -> {
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

    protected void awaitForOneLoopCycle() {
        awaitForSomeLoopCycles(1);
    }

    protected void awaitForSomeLoopCycles(int thisManyMore) {
        log.debug("Waiting for {} more iterations of the control loop.", thisManyMore);
        blockingLoopLatchTrigger(thisManyMore);
        log.debug("Completed waiting on {} loop(s)", thisManyMore);
    }

    protected void awaitUntilTrue(Callable<Boolean> booleanCallable) {
        waitAtMost(defaultTimeout).until(booleanCallable);
    }

    /**
     * Make sure the latch is attached, if this times out unexpectedly
     */
    @SneakyThrows
    private void blockingLoopLatchTrigger(int waitForCount) {
        log.debug("Waiting on {} cycles on loop latch for {}...", waitForCount, defaultTimeout);
        loopLatchV = new CountDownLatch(waitForCount);
        try {
            boolean timeout = !loopLatchV.await(defaultTimeoutSeconds, SECONDS);
            if (timeout || parentParallelConsumer.isClosedOrFailed())
                throw new TimeoutException(msg("Timeout of {}, waiting for {} counts, on latch with {} left", defaultTimeout, waitForCount, loopLatchV.getCount()));
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for loop latch - timeout was {}", defaultTimeout);
            throw e;
        }
    }

    @SneakyThrows
    private void awaitForLoopCount(int waitForCount) {
        log.debug("Waiting on {} cycles on loop latch...", waitForCount);
        waitAtMost(defaultTimeout.multipliedBy(100)).until(() -> loopCount.get() > waitForCount);
    }

    protected void awaitForCommit(int offset) {
        log.debug("Waiting for commit offset {}", offset);
        await().timeout(defaultTimeout)
                .untilAsserted(() -> assertCommitsContains(of(offset)));
    }

    protected void awaitForCommitExact(int offset) {
        log.debug("Waiting for EXACTLY commit offset {}", offset);
        await().timeout(defaultTimeout)
                .failFast(msg("Commit was not exact - contained offsets that weren't '{}'", offset), () -> {
                    List<Integer> offsets = extractAllPartitionsOffsetsSequentially(false);
                    return offsets.size() > 1 && !offsets.contains(offset);
                })
                .untilAsserted(() -> assertCommits(of(offset)));
    }

    protected void awaitForCommitExact(int partition, int offset) {
        log.debug("Waiting for EXACTLY commit offset {} on partition {}", offset, partition);
        var expectedOffset = new OffsetAndMetadata(offset, "");
        TopicPartition partitionNumber = new TopicPartition(INPUT_TOPIC, partition);
        var expectedOffsetMap = UniMaps.of(partitionNumber, expectedOffset);
        verify(producerSpy, timeout(defaultTimeoutMs)
                .times(1))
                .sendOffsetsToTransaction(
                        argThat((offsetMap) -> offsetMap.equals(expectedOffsetMap)),
                        any(ConsumerGroupMetadata.class));
    }

    public void assertCommitsContains(List<Integer> offsets) {
        List<Integer> commits = getCommitHistoryFlattened();
        assertThat(commits).containsAll(offsets);
    }

    protected List<Integer> getCommitHistoryFlattened() {
        return (isUsingTransactionalProducer())
                ? ktu.getProducerCommitsFlattened(producerSpy)
                : extractAllPartitionsOffsetsSequentially(false);
    }

    private List<OffsetAndMetadata> getCommitHistoryFlattenedMeta() {
        return (isUsingTransactionalProducer())
                ? ktu.getProducerCommitsMeta(producerSpy)
                : extractAllPartitionsOffsetsSequentiallyMeta(true);
    }

    public void assertCommits(List<Integer> offsets, String description) {
        assertCommits(offsets, Optional.of(description));
    }

    /**
     * Flattens the offsets of all partitions into a single sequential list. Removing the genesis commit (0) if it
     * exists, unless it's contained in the assertion.
     */
    public void assertCommits(List<Integer> offsets, Optional<String> description) {
        boolean trimGenesis = !offsets.contains(0);

        if (isUsingTransactionalProducer()) {
            ktu.assertCommits(producerSpy, offsets, description);
            assertThat(extractAllPartitionsOffsetsSequentially(trimGenesis)).isEmpty();
        } else {
            List<Integer> collect = extractAllPartitionsOffsetsSequentially(trimGenesis);

            // duplicates are ok
            // is there a nicer optional way?
            // {@link Optional#ifPresentOrElse} only @since 9
            if (description.isPresent()) {
                assertThat(collect).as(description.get()).hasSameElementsAs(offsets);
            } else {
                assertThat(collect).hasSameElementsAs(offsets);
            }
            ktu.assertCommits(producerSpy, UniLists.of(), Optional.of("Empty"));
        }
    }

    /**
     * Flattens the offsets of all partitions into a single sequential list
     */
    protected List<Integer> extractAllPartitionsOffsetsSequentially(boolean trimGenesis) {
        return extractAllPartitionsOffsetsSequentiallyMeta(trimGenesis).stream().
                map(x -> (int) x.offset()) // int cast a luxury in test context - no big offsets
                .collect(Collectors.toList());
    }

    /**
     * Flattens the offsets of all partitions into a single sequential list
     */
    protected List<OffsetAndMetadata> extractAllPartitionsOffsetsSequentiallyMeta(boolean trimGenesis) {
        // copy the list for safe concurrent access
        List<Map<TopicPartition, OffsetAndMetadata>> history = new ArrayList<>(consumerSpy.getCommitHistoryInt());
        return history.stream()
                .flatMap(commits ->
                        {
                            var rawValues = new ArrayList<>(commits.values()).stream(); // 4 debugging
                            if (trimGenesis)
                                return rawValues.filter(x -> x.offset() != 0);
                            else
                                return rawValues; // int cast a luxury in test context - no big offsets
                        }
                ).collect(Collectors.toList());
    }


    protected List<OffsetAndMetadata> extractAllPartitionsOffsetsAndMetadataSequentially() {
        // copy the list for safe concurrent access
        List<Map<TopicPartition, OffsetAndMetadata>> history = new ArrayList<>(consumerSpy.getCommitHistoryInt());
        return history.stream()
                .flatMap(commits ->
                        {
                            Collection<OffsetAndMetadata> values = new ArrayList<>(commits.values());
                            return values.stream();
                        }
                ).collect(Collectors.toList());
    }

    public void assertCommits(List<Integer> offsets) {
        assertCommits(offsets, Optional.empty());
    }

    public CommitHistorySubject assertCommits() {
        List<OffsetAndMetadata> commitHistoryFlattened = getCommitHistoryFlattenedMeta();
        CommitHistory actual = new CommitHistory(commitHistoryFlattened);
        return CommitHistorySubject.assertThat(actual);
    }

    /**
     * Checks a list of commits of a list of partitions - outer list is partition, inner list is commits
     */
    public void assertCommitLists(List<List<Integer>> offsets) {
        if (isUsingTransactionalProducer()) {
            ktu.assertCommitLists(producerSpy, offsets, Optional.empty());
        } else {
            List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> commitHistoryWithGropuId = consumerSpy.getCommitHistoryWithGroupId();
            ktu.assertCommitLists(commitHistoryWithGropuId, offsets, Optional.empty());
        }
    }

    protected List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> getCommitHistory() {
        if (isUsingTransactionalProducer()) {
            return producerSpy.consumerGroupOffsetsHistory();
        } else {
            return consumerSpy.getCommitHistoryWithGroupId();
        }
    }

    protected boolean isUsingTransactionalProducer() {
        ParallelConsumerOptions.CommitMode commitMode = parentParallelConsumer.getWm().getOptions().getCommitMode();
        return commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER);
    }

    protected boolean isUsingAsyncCommits() {
        ParallelConsumerOptions.CommitMode commitMode = parentParallelConsumer.getWm().getOptions().getCommitMode();
        return commitMode.equals(PERIODIC_CONSUMER_ASYNCHRONOUS);
    }

    protected void releaseAndWait(List<CountDownLatch> locks, List<Integer> lockIndexes) {
        for (Integer i : lockIndexes) {
            log.debug("Releasing {}...", i);
            locks.get(i).countDown();
        }
        awaitForSomeLoopCycles(1);
    }

    protected void releaseAndWait(List<CountDownLatch> locks, int lockIndex) {
        log.debug("Releasing {}...", lockIndex);
        locks.get(lockIndex).countDown();
        awaitForSomeLoopCycles(1);
    }

    protected void pauseControlToAwaitForLatch(CountDownLatch latch) {
        pauseControlLoop();
        awaitLatch(latch);
        resumeControlLoop();
        awaitForOneLoopCycle();
    }

    /**
     * Assert {@link com.google.common.truth.Truth} on the test {@link Consumer} ({@link LongPollingMockConsumer}).
     */
    protected LongPollingMockConsumerSubject<String, String> assertThatConsumer(String msg) {
        return Truth.assertWithMessage(msg)
                .about(LongPollingMockConsumerSubject.<String, String>mockConsumers())
                .that(consumerSpy);
    }
}
