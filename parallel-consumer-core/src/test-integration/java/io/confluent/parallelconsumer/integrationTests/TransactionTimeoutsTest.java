package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.csid.utils.LatchTestUtils;
import io.confluent.csid.utils.ThreadUtils;
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
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.REUSE_GROUP;
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

    public static final int SMALL_TIMEOUT = 3;

    private ParallelEoSStreamProcessor<String, String> pc;

    BrokerCommitAsserter broker;

    @SneakyThrows
    void setup(PCModule<String, String> module) {
        setupTopic(TransactionTimeoutsTest.class.getSimpleName());

        pc = new ParallelEoSStreamProcessor<>(module.options(), module);

        kcu.produceMessages(getTopic(), NUMBER_TO_SEND);

        pc.subscribe(of(getTopic()));

        broker = new BrokerCommitAsserter(getKcu(), getTopic());
    }

    private ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> createOptions() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(kcu.createNewConsumer())
                .producer(kcu.createNewProducer(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER))
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .commitLockAcquisitionTimeout(ofSeconds(2))
                .defaultMessageRetryDelay(ofMillis(100))
                .produceLockAcquisitionTimeout(ofSeconds(2))
                .timeBetweenCommits(ofSeconds(NUMBER_TO_SEND))
                .allowEagerProcessingDuringTransactionCommit(true);
    }

    /**
     * Sleep time multiplier:
     * <p>
     * 5: triggers a timeout
     * <p>
     * 50: triggers a timeout with a much longer deadlock
     *
     * @param multiple
     */
    @SneakyThrows
    @ParameterizedTest()
    @ValueSource(ints = {
            SMALL_TIMEOUT, // triggers a timeout, but get's committed in the shutdown commit
            50 // a much longer deadlock, which is still blocked at shutdown, and so shutdown interrupts the sleep, but it never got succeeded, so when shutdown commit runs, it successdully commits, but base offset will be zero, with the incomplete data encoded.
    })
    void commitTimeout(int multiple) {
        var options = createOptions()
                .allowEagerProcessingDuringTransactionCommit(false)
                .build();
        setup(new PCModule<>(options));

        final int offsetToFail = 3;
        pc.pollAndProduce(recordContexts -> {
            long offset = recordContexts.offset();
            if (offset == offsetToFail) {
                // triggers deadlock as controller can't acquire commit lock fast enough due to this sleeping thread
                ThreadUtils.sleepQuietly(1000 * multiple);
            }
            return new ProducerRecord<>(getTopic() + "-output", "random");
        });


        // send 1 more
        getKcu().produceMessages(getTopic(), 2);

        // assert output topic only
        var target = 1;
        broker.assertConsumedOffset(target);

        // send 1 more, upon which offset the pc function will block forever, causing a commit timeout
        getKcu().produceMessages(getTopic(), 2);

        // wait until pc dies from commit timeout
        await().untilAsserted(() -> assertThat(pc).isClosedOrFailed());
        assertThat(pc).getFailureCause().hasMessageThat().contains("timeout");


        // check what exists in the output topic
        // 2nd commit will have succeeded
        var newConsumer = kcu.createNewConsumer(REUSE_GROUP);
        newConsumer.subscribe(of(getTopic()));
        newConsumer.poll(ofSeconds(20));

        var commitHistorySubject = assertThat(newConsumer).hasCommittedToPartition(new TopicPartition(getTopic(), offsetToFail));

        commitHistorySubject.encodedIncomplete(0);
        commitHistorySubject.offset(0);
    }

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
                    ThreadUtils.sleepQuietly(10000);
                }

                return invocation.callRealMethod();
            }
        };
        setup(slowCommitModule);


        AtomicInteger retryCount = new AtomicInteger();
        pc.pollAndProduce(recordContexts -> {
            long offset = recordContexts.offset();
            ThreadUtils.sleepQuietly(1000);
            if (offset == 4) {
                retryCount.set(recordContexts.getSingleRecord().getNumberOfFailedAttempts());
                LatchTestUtils.awaitLatch(produceLock);
                ThreadUtils.sleepQuietly(1000);
            }
            return new ProducerRecord<>(getTopic() + "-output", "random");
        });

        //// send, process, commit
        // assert output topic
        broker.assertConsumedOffset(5); // happy path, all base records committed ok

        //// send, process, block, retry, succeed
        // send 3 more records
        getKcu().produceMessages(getTopic(), 3);

        // assert output topic - has got the new records due to commit blocked
        broker.assertConsumedOffset(5); // happy path, all base records committed ok

        // wait for retry
        await().atMost(ofSeconds(60)).untilAsserted(() -> Truth.assertThat(retryCount.get()).isAtLeast(1));

        // assert output topic
        broker.assertConsumedOffset(8); // happy path after retry, all records committed and read ok

    }

}
