package io.confluent.csid.asyncconsumer;

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.LongPollingMockConsumer;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.confluent.csid.utils.Range.range;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.waitAtMost;
import static org.mockito.Mockito.*;

@Slf4j
public class AsyncConsumerTestBase {

    public static final String INPUT_TOPIC = "input";
    public static final String OUTPUT_TOPIC = "output";
    public static final String CONSUMER_GROUP_ID = "my-group";

    protected MockConsumer<String, String> consumerSpy;
    protected MockProducer<String, String> producerSpy;

    protected AsyncConsumer<String, String> asyncConsumer;

    static protected int defaultTimeoutSeconds = 5;

    static protected Duration defaultTimeout = ofSeconds(defaultTimeoutSeconds);
    static protected long defaultTimeoutMs = defaultTimeout.toMillis();
    static protected Duration infiniteTimeout = Duration.ofMinutes(20);

    AsyncConsumerTest.MyAction myRecordProcessingAction;

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
        setupAsyncConsumerInstance(AsyncConsumerOptions.builder().build());
    }

    protected List<WorkContainer<String, String>> successfulWork = Collections.synchronizedList(new ArrayList<>());

    private void setupWorkManager(WorkManager<String, String> wm) {
        wm.getSuccessfulWorkListeners().add((work)->{
            log.debug("Heard some successful work: {}", work);
            successfulWork.add(work);
        });
    }

    protected void primeFirstRecord() {
        firstRecord = ktu.makeRecord("key-0", "v0");
        consumerSpy.addRecord(firstRecord);
    }

    protected MockConsumer<String, String> setupClients() {
        MockConsumer<String, String> consumer = new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);
        MockProducer<String, String> producer = new MockProducer<>(true, null, null); // TODO do async testing

        this.producerSpy = spy(producer);
        this.consumerSpy = spy(consumer);
        myRecordProcessingAction = mock(AsyncConsumerTest.MyAction.class);

        ktu = new KafkaTestUtils(consumerSpy);

        KafkaTestUtils.setupConsumer(this.consumerSpy);

        return consumerSpy;
    }

    protected void setupAsyncConsumerInstance(AsyncConsumerOptions.ProcessingOrder order) {
        setupAsyncConsumerInstance(AsyncConsumerOptions.builder().ordering(order).build());
    }

    protected void setupAsyncConsumerInstance(AsyncConsumerOptions asyncConsumerOptions) {
        setupClients();

        asyncConsumer = initAsyncConsumer(asyncConsumerOptions);

        asyncConsumer.setLongPollTimeout(ofMillis(100));
        asyncConsumer.setTimeBetweenCommits(ofMillis(100));

        verificationWaitDelay = asyncConsumer.getTimeBetweenCommits().multipliedBy(2).toMillis();

        loopCountRef = attachLoopCounter(asyncConsumer);

        setupWorkManager(asyncConsumer.getWm());
    }

    protected AsyncConsumer<String, String> initAsyncConsumer(AsyncConsumerOptions asyncConsumerOptions) {
        asyncConsumer = new AsyncConsumer<>(consumerSpy, producerSpy, asyncConsumerOptions);

        return asyncConsumer;
    }

    protected void sendSecondRecord(MockConsumer<String, String> consumer) {
        secondRecord = ktu.makeRecord("key-0", "v1");
        consumer.addRecord(secondRecord);
    }

    protected AtomicReference<Integer> attachLoopCounter(AsyncConsumer asyncConsumer) {
        final AtomicReference<Integer> currentLoop = new AtomicReference<>(0);
        asyncConsumer.addLoopEndCallBack(() -> {
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

    @SneakyThrows
    private void blockingLoopLatchTrigger(int waitForCount) {
        log.debug("Waiting on {} cycles on loop latch...", waitForCount);
        loopLatchV = new CountDownLatch(waitForCount);
        loopLatchV.await(defaultTimeoutSeconds, TimeUnit.SECONDS);
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

    public void assertCommits(List<Integer> integers, String description) {
        KafkaTestUtils.assertCommits(producerSpy, integers, Optional.of(description));
    }

    public void assertCommits(List<Integer> integers) {
        KafkaTestUtils.assertCommits(producerSpy, integers, Optional.empty());
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
