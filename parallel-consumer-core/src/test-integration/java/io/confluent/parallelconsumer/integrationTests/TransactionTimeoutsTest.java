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
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import io.confluent.parallelconsumer.internal.ProducerManager;
import io.confluent.parallelconsumer.internal.ProducerWrap;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import pl.tlinkowski.unij.api.UniLists;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Duration.ofSeconds;
import static org.mockito.ArgumentMatchers.any;

/**
 * Tests behaviour under timeouts
 *
 * @see ProducerManager
 */
@Tag("transactions")
@Slf4j
class TransactionTimeoutsTest extends BrokerIntegrationTest<String, String> {

    private ParallelEoSStreamProcessor<String, String> pc;

    @SneakyThrows
    void setup(PCModule module) {
        setupTopic(TransactionTimeoutsTest.class.getSimpleName());

        pc = new ParallelEoSStreamProcessor<>(module.options(), module);

        kcu.produceMessages(getTopic(), 5);

        pc.subscribe(UniLists.of(getTopic()));
    }

    private ParallelConsumerOptions<String, String> createOptions() {
        ParallelConsumerOptions<String, String> options = ParallelConsumerOptions.<String, String>builder()
                .consumer(kcu.createNewConsumer())
                .producer(kcu.createNewProducer(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER))
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .commitLockAcquisitionTimeout(ofSeconds(2))
                .produceLockAcquisitionTimeout(ofSeconds(2))
                .timeBetweenCommits(ofSeconds(5))
                .allowEagerProcessingDuringTransactionCommit(true)
                .build();
        return options;
    }

    @Test
    void commitTimeout() {
        setup(new PCModuleTestEnv(createOptions()));

        // send, process, commit
        // assert output topic

        // send, process, block


        // wait for timeout death

        // assert output topic


        pc.pollAndProduce(recordContexts -> {
            long offset = recordContexts.offset();
            if (offset == 0) {
                // triggers deadlock
                ThreadUtils.sleepQuietly(5000);
            }
            return new ProducerRecord<>(getTopic() + "-output", "random");
        });

        //
        Awaitility.await().until(() -> pc.getFailureCause() != null);

        // check what exists in the output topic
    }

    @Test
    void produceTimeout() {

        CountDownLatch produceLock = new CountDownLatch(1);

        PCModule<String, String> slowCommitModule = new PCModule<>(createOptions()) {

            @Override
            protected ProducerWrap<String, String> producerWrap() {
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

        // send, process, commit
        // assert output topic

        // send, process, block

        // wait for timeout death

        // assert output topic

        Awaitility.await().atMost(ofSeconds(60)).untilAsserted(() -> Truth.assertThat(retryCount.get()).isAtLeast(1));

        // assert output topic

    }

}
