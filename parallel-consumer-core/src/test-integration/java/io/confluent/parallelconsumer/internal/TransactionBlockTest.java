package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.integrationTests.TransactionMarkersTest;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ManagedTruth.assertWithMessage;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.NEW_GROUP;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Tests around ensuring the Producer system blocks further work collection and record sending through the commit phase
 *
 * @author Antony Stubbs
 * @see ProducerManagerTest
 */
@Tag("transactions")
@Tag("#355")
@Slf4j
        // todo implement or delete - all covered in ProducerManagerTest
// todo extend broker integration test instead
class TransactionBlockTest extends TransactionMarkersTest {

    /**
     * Producer blocks any further work being started or records being sent during commit phase
     */
    // todo implement or delete
    @Disabled
    @Test
    void testProducerLock() {
        var isolationCommittedConsumer = kcu.createNewConsumer(NEW_GROUP);

        // send source messages
        int numRecords = 3;
        int blockFreeRecords = numRecords - 1;
        int blockedOffset = numRecords - 1;
        sendRecordsNonTransactionallyAndBlock(numRecords);

        var blockedRec = new CountDownLatch(1);

        // process 3 records, sending 2 from each
        pc.pollAndProduceMany(recordContexts -> {
            if (recordContexts.offset() == blockedOffset) {
                // todo use waiter
                try {
                    blockedRec.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            return makeOutput(recordContexts);
        });


        // start committing transaction
        pc.requestCommitAsap();

        // somehow block tx from completing

        // while committing tx, try to produce another record, observe it's blocked
        {
            // unblock
            blockedRec.countDown();
            pc.requestCommitAsap();
            // assert for 1 second
            await().pollDelay(Duration.ofSeconds(1)).untilAsserted(() ->
            {
                var poll = isolationCommittedConsumer.poll(Duration.ZERO);
                assertWithMessage("Output topic missing blocked record").that(poll).doesntContainOffset(blockedOffset);
            });
        }

        // allow finish transaction
//        ???
        Truth.assertThat(true).isFalse();


        // assert tx completes
        await().untilAsserted(() ->
        {
            var poll = isolationCommittedConsumer.poll(Duration.ZERO);
            assertThat(poll).containsOffset(blockedOffset);
        });

// delete
//        // commit open transaction
////        ???
//        Truth.assertThat(true).isFalse();
//
//        // assert results topic contains all
//        assertThat(pc).hasCommittedToAnything(blockFreeRecords);
//        {
//            var poll = isolationCommittedConsumer.poll(Duration.ZERO);
//            assertThat(poll).containsOffset(blockedOffset);
//        }
    }

    /**
     * Test aborting the second tx has only first plus nothing in result topic
     */
    @Test
    // todo implement or delete
    @Disabled
    void abortedSecondTransaction() {
        Truth.assertThat(true).isFalse();
    }

    /**
     * Test aborting the first tx ends up with nothing
     */
    @Test
    // todo implement or delete
    @Disabled
    void abortedBothTransactions() {
        // do the above again, but instead abort the transaction
        // assert nothing on result topic
        Truth.assertThat(true).isFalse();
    }

    /**
     * Test option to drain consumer records (work records) that are in flight (but haven't produced a record yet,
     * otherwise they already be drained in the producer flush) while blocking any further work from being started.
     */
    @Test()
    // todo implement or delete
    @Disabled("Not implemented")
    void drainInflightWork() {
    }

    class MyAdvice {
        @Advice.OnMethodEnter
        public void enter() {
            log.error("hi");
        }
    }

    /**
     * test that uses mocks to ensure the correct methods are being called, and to make the transaction committing very
     * slow
     */
    @Test
    @Disabled
    // todo delete - effectively done in the ProducerManagerTest's
    void mockTest() {
        Class<?> dynamicType = new ByteBuddy()
                .subclass(Object.class)
                .method(named("toString")).intercept(FixedValue.value("Hello World!"))
                .make()
                .load(getClass().getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
                .getLoaded();
//        dynamicType.

//        final Instrumentation install = ByteBuddyAgent.install().redefineClasses();


        var targetPackageName = PollContext.class.getPackageName();
        final AgentBuilder.Default aDefault = new AgentBuilder.Default();
        aDefault.with(AgentBuilder.Listener.StreamWriting.toSystemOut());
        aDefault.type(ElementMatchers.nameStartsWith(targetPackageName))
                .transform((builder, typeDescription, classLoader, javaModule) -> {
                    return builder
                            .visit(Advice.to(MyAdvice.class).on(ElementMatchers.isMethod()));
                });

        final PollContext<Object, Object> recordContexts = new PollContext<>();
        recordContexts.offset();

        log.debug("end");


//        ProducerManager<String, String> pm = Mockito.<ProducerManager<String, String>>mock(ProducerManager.class);
//        Mockito.when(pm.produceMessages()).
//        List<ParallelConsumer.Tuple<ProducerRecord<String, String>, Future<RecordMetadata>>> tuples = pm.produceMessages(List.of());
    }

    @NonNull
    private List<ProducerRecord<String, String>> makeOutput(PollContext<String, String> recordContexts) {
        return recordContexts.stream()
                .map(record
                        -> new ProducerRecord<String, String>(getTopic(), record.value()))
                .collect(Collectors.toList());
    }

    // todo move to producer utils
    private void sendRecordsNonTransactionallyAndBlock(int i) {
        sendRecordsNonTransactionally(3).forEach(future -> {
            try {
                future.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
