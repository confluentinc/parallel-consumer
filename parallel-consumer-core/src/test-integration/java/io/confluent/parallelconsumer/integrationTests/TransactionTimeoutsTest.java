package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.csid.utils.LatchTestUtils;
import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.FakeRuntimeError;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.integrationTests.utils.BrokerCommitAsserter;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.internal.ProducerManager;
import io.confluent.parallelconsumer.internal.ProducerWrapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.mockito.ArgumentMatchers.any;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Tests behaviour under timeouts
 *
 * @author Antony Stubbs
 * @see ProducerManager
 * @see io.confluent.parallelconsumer.internal.ProducerManagerTest
 */
@Tag("transactions")
@Slf4j
class TransactionTimeoutsTest extends BrokerIntegrationTest<String, String> {

    public static final int NUMBER_TO_SEND = 5;

    public static final int SMALL_TIMEOUT = 2;

    private ParallelEoSStreamProcessor<String, String> pc;

    private String originalGroupId;

    BrokerCommitAsserter assertConsumer;

    @SneakyThrows
    void setup(PCModule<String, String> module) {
        setupTopic(TransactionTimeoutsTest.class.getSimpleName());

        pc = new ParallelEoSStreamProcessor<>(module.options(), module);

        kcu.produceMessages(getTopic(), NUMBER_TO_SEND);

        pc.subscribe(of(getTopic()));

        originalGroupId = getKcu().getConsumer().groupMetadata().groupId();

        // todo losses the original group id
        assertConsumer = new BrokerCommitAsserter(getTopic(), getKcu().createNewConsumer());
    }

    private ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> createOptions() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(kcu.createNewConsumer())
                .producer(kcu.createNewProducer(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER))
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .commitLockAcquisitionTimeout(ofSeconds(1))
                .defaultMessageRetryDelay(ofMillis(100))
                .produceLockAcquisitionTimeout(ofSeconds(2))
                .timeBetweenCommits(ofSeconds(1))
                .allowEagerProcessingDuringTransactionCommit(true);
    }

    /**
     * Tests what happens with the commit stage times out.
     * <p>
     * First sends {@link #NUMBER_TO_SEND} and allows them to commit cleanly. Then sends more records of which one takes
     * too long to process, causing the commit to timeout.
     * <p>
     * Runs with different injected timeout sizes:
     * <p>
     * Sleep time multiplier:
     * <p>
     * 5: triggers a timeout
     * <p>
     * 50: triggers a timeout with a much longer deadlock
     *
     * @param multiple Multiple values - but affect is the same. It's not worth trying to artifically create a scneario
     *                 where the sleep wakes up /after/ the commit lock has timed out - this would affectively be a semi
     *                 happy path, where the result record is produced, in time for the shutdown commit, or times out
     *                 the shutdown commit as well and so the transaction doesn't get committed and will eventually
     *                 abort:
     *                 <p>
     *                 Small value: triggers a timeout, but gets committed in the shutdown commit, with the incomplete
     *                 offsets correct - as the sleep gets interrupted by the shutdown process (and so result record
     *                 never produced), marked as failed and committed as such.
     *                 <p>
     *                 Large value: same as the small version, as the sleep is also interrupted.
     */
    @SneakyThrows
    @ParameterizedTest()
    @ValueSource(ints = {
            SMALL_TIMEOUT,
            50
    })
    void commitTimeout(int multiple) {
        var options = createOptions()
                .allowEagerProcessingDuringTransactionCommit(false)
                .build();
        setup(new PCModule<>(options));

        // allow the first offsets to succeed, which we can test
        final int offsetToGoVerySlow = NUMBER_TO_SEND + 3;

        String outputTopic = getTopic() + "-output";
        int offsetToError = 12;

        pc.pollAndProduce(recordContexts -> {
            log.debug("Processing {}", recordContexts.offset());
            long offset = recordContexts.offset();
            if (offset == offsetToGoVerySlow) {
                // triggers deadlock as controller can't acquire commit lock fast enough due to this sleeping thread
                log.debug("Processing offset {} - simulating a long processing phase with timeout multiple {}", offsetToGoVerySlow, multiple);
                ThreadUtils.sleepQuietly(1000 * multiple);
                log.debug("Processing offset {} - simulating a long processing phase COMPLETE", offsetToGoVerySlow);
            } else if (offset == offsetToError) {
                throw new FakeRuntimeError("fail");
            }
            return new ProducerRecord<>(outputTopic, "output-value,source-offset: " + offset);
        });

        // assert output topic contains records from processing function - so commit must have committed cleanly
        var target = NUMBER_TO_SEND - 1;
        assertConsumer.assertConsumedOffset(outputTopic, target); // check a new consumer group can consme the records expected to be committed to the partition

        // send more, upon which offset the pc function will block forever, causing a commit timeout
        getKcu().produceMessages(getTopic(), 10);

        pc.requestCommitAsap();

        // wait until pc dies from commit timeout
        await().untilAsserted(() -> assertThat(pc).isClosedOrFailed());
        assertThat(pc).getFailureCause().hasMessageThat().contains("timeout");

        // check what was committed at shutdown to the input topic, re-using same group id as PC, to access what was committed at shutdown commit attempt
        // 2nd commit attempt during shutdown will have succeeded
        var newConsumer = kcu.createNewConsumer(originalGroupId);
        var assertCommittedToPartition = assertThat(newConsumer).hasCommittedToPartition(getTopic(), partitionNumber);

        assertCommittedToPartition.offset(offsetToGoVerySlow);
        assertCommittedToPartition.encodedIncomplete(offsetToGoVerySlow, offsetToError);
    }

    /**
     * Tests what happens when the record sending stage times out.
     */
    @SneakyThrows
    @Test
    void produceTimeout() {

        CountDownLatch produceLock = new CountDownLatch(1);

        // inject system that causes commit to take too long
        PCModule<String, String> slowCommitModule = new PCModule<>(createOptions().build()) {

            @Override
            protected ProducerWrapper<String, String> producerWrap() {
                var pw = Mockito.spy(super.producerWrap());

                // inject a long sleep in the commit flow
                Mockito.doAnswer(this::maybeSleep)
                        .when(pw)
                        .sendOffsetsToTransaction(ArgumentMatchers.anyMap(), any(ConsumerGroupMetadata.class));

                return pw;
            }

            @SneakyThrows
            private Object maybeSleep(InvocationOnMock invocation) {
                // only timeout on 2nd commit
                Map<TopicPartition, OffsetAndMetadata> offsets = (Map<TopicPartition, OffsetAndMetadata>) (invocation.getArguments()[0]);
                OffsetAndMetadata offsetAndMetadata = offsets.get(new TopicPartition(getTopic(), 0));
                long offset = offsetAndMetadata.offset();

                if (offset == 4) {
                    log.debug("Causing commit to take too long which will trigger produce lock timeout");
                    produceLock.countDown();
                    ThreadUtils.sleepQuietly(10000); // sleep for 10 seconds to simulate timeout
                }

                return invocation.callRealMethod();
            }
        };
        setup(slowCommitModule);

        //
        AtomicInteger retryCount = new AtomicInteger();
        pc.pollAndProduce(recordContexts -> {
            long offset = recordContexts.offset();
            ThreadUtils.sleepQuietly(1000);
            if (offset == 4) {
                retryCount.set(recordContexts.getSingleRecord().getNumberOfFailedAttempts());
                LatchTestUtils.awaitLatch(produceLock);
                ThreadUtils.sleepQuietly(1000); // block offset 4 for a second
            }
            return new ProducerRecord<>(getTopic() + "-output", "random");
        });

        //// send, process, commit
        // assert output topic
        assertConsumer.assertConsumedOffset(5); // happy path, all base records committed ok

        //// send, process, block, retry, succeed
        // send 3 more records
        getKcu().produceMessages(getTopic(), 3);

        // assert output topic - has got the new records due to commit blocked
        assertConsumer.assertConsumedOffset(5); // happy path, all base records committed ok

        // wait for retry
        await().atMost(ofSeconds(60)).untilAsserted(() -> Truth.assertThat(retryCount.get()).isAtLeast(1));

        // assert output topic
        assertConsumer.assertConsumedOffset(8); // happy path after retry, all records committed and read ok
    }

}
