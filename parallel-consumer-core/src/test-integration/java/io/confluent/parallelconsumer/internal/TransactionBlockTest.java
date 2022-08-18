package io.confluent.parallelconsumer.internal;

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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.NEW_GROUP;
import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
@Tag("transactions")
@Tag("#355")
@Slf4j
class TransactionBlockTest extends TransactionMarkersTest {

    @Test
    void testProducerLock() {
        // send source messages
        int numRecords = 3;
        int blockFreeRecords = numRecords - 1;
        int blockedOffset = numRecords - 1;
        sendRecordsNonTransactionallyAndBlock(numRecords);

        var blockedRec = new CountDownLatch(1);

        // process two records, sending 3 from each
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

        // assert tx completes
        var isolationCommittedConsumer = kcu.createNewConsumer(NEW_GROUP);
        {
            var poll = isolationCommittedConsumer.poll(Duration.ZERO);
            assertThat(poll).containsOffset(blockFreeRecords);
        }

        // while sending tx, try to produce another record, observe it's blocked
        // unblock
        blockedRec.countDown();
        // assert for 1 second
        pc.requestCommitAsap();
        {
            var poll = isolationCommittedConsumer.poll(Duration.ZERO);
            assertThat(poll).doesntContainOffset(blockedOffset);
        }

        // finish transaction
//        ???
        Truth.assertThat(true).isFalse();

        // assert blocked record now sent
        pc.requestCommitAsap();
        assertThat(pc).hasCommittedToAnything(blockFreeRecords);
        {
            var poll = isolationCommittedConsumer.poll(Duration.ZERO);
            assertThat(poll).containsOffset(blockedOffset);
        }

        // commit open transaction
//        ???
        Truth.assertThat(true).isFalse();

        // assert results topic contains all
        assertThat(pc).hasCommittedToAnything(blockFreeRecords);
        {
            var poll = isolationCommittedConsumer.poll(Duration.ZERO);
            assertThat(poll).containsOffset(blockedOffset);
        }
    }

    @Test
    void abortedTransaction() {
        // do the above again, but instead abort the transaction
        // assert nothing on result topic
        // retry
        // assert results in output topic
        Truth.assertThat(true).isFalse();
    }

    class MyAdvice {
        @Advice.OnMethodEnter
        public void enter() {
            log.error("hi");
        }
    }

    @Test
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
