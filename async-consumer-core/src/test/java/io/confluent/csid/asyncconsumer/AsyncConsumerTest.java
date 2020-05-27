package io.confluent.csid.asyncconsumer;

import io.confluent.csid.utils.KafkaUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.lang.invoke.VarHandle;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.KafkaUtils.toTP;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.List.of;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@Slf4j
public class AsyncConsumerTest extends AsyncConsumerTestBase {

    static class MyAction implements Function<ConsumerRecord<MyKey, MyInput>, String> {

        @Override
        public String apply(ConsumerRecord<MyKey, MyInput> record) {
            log.info("User client function - consuming a record... {}", record.key());
            return "my-result";
        }
    }

    long wait;

    @BeforeEach
    public void setupAsyncConsumerTest() {
        asyncConsumer = new AsyncConsumer<MyKey, MyInput>(consumerSpy, producerSpy);
        asyncConsumer.setLongPollTimeout(ofMillis(100));
        asyncConsumer.setTimeBetweenCommits(ofMillis(100));

        wait = asyncConsumer.getTimeBetweenCommits().multipliedBy(2).toMillis();
    }

    @Test
    @SneakyThrows
    public void failingActionNothingCommitted() {
        var reentrantLock = new CountDownLatch(2);
        asyncConsumer.asyncVoidPoll((ignore) -> {
            reentrantLock.countDown();
            throw new RuntimeException("My user's function error");
        });
        reentrantLock.await(defaultTimeoutSeconds, SECONDS);

        verify(producerSpy, times(0)).commitTransaction();

        asyncConsumer.waitForNoInFlight(defaultTimeout);

        verify(producerSpy, times(0)).commitTransaction();

        asyncConsumer.close();
    }

    @Test
    @SneakyThrows
    public void offsetsAreNeverCommittedForMessagesStillInFlightSimplest() {
        var msg0Lock = new CountDownLatch(1);
        var msg1Lock = new CountDownLatch(1);

        // finish processing only msg 1
        asyncConsumer.asyncVoidPoll((ignore) -> {
            int offset = (int) ignore.offset();
            switch (offset) {
                case 0 -> {
                    // wait for processing
                    try {
                        msg0Lock.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                case 1 -> {
                    // wait for processing
                    try {
                        msg1Lock.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        // finish processing 1
        msg1Lock.countDown();

        // make sure no offsets are committed
        verify(producerSpy, after(wait).never()).commitTransaction(); // todo smelly, change to event based - this is racey

        // for other tests,
        // finish 0
        // todo prefer to shutdown and verify, but no mechanism presently as messages are still in "flight"
//        asyncConsumer.close();
//        verify(producerSpy, never()).commitTransaction(); // todo smelly, change to event based - this is racey
//        msg0Lock.await();
        log.debug("Unlocking 0");
        msg0Lock.countDown();
        asyncConsumer.close();
    }

    @Test
//    @SneakyThrows
    public void offsetsAreNeverCommittedForMessagesStillInFlightShort() throws InterruptedException {
        offsetsAreNeverCommittedForMessagesStillInFlightSimplest();

        // make sure offset 1, not 0 is committed
        // check only 1 is now committed, not committing 0 as well is a performance thing
        verify(producerSpy,
                after(wait)
                        .times(1)
                        .description("Only one of the two offsets committed for efficiency"))
                .commitTransaction();


        assertCommits(of(1));
    }

    @Test
//    @SneakyThrows
    public void offsetsAreNeverCommittedForMessagesStillInFlightLong() throws InterruptedException {
        // send three messages - 0, 1, 2
        consumer.addRecord(makeRecord("0", "v2"));
        consumer.addRecord(makeRecord("0", "v3"));
        consumer.addRecord(makeRecord("0", "v4"));
        consumer.addRecord(makeRecord("0", "v5"));

        var msg0Lock = new CountDownLatch(1);
        var msg1Lock = new CountDownLatch(1);
        var msg2Lock = new CountDownLatch(1);
        var msg3Lock = new CountDownLatch(1);
        var msg4Lock = new CountDownLatch(1);
        var msg5Lock = new CountDownLatch(1);

        List<CountDownLatch> locks = of(msg0Lock, msg1Lock, msg2Lock, msg3Lock, msg4Lock, msg5Lock);

        // finish processing only 1

        CountDownLatch startLatch = new CountDownLatch(1);

        asyncConsumer.asyncVoidPoll((ignore) -> {
            int offset = (int) ignore.offset();
            CountDownLatch latchForMsg = locks.get(offset);
            try {
                startLatch.countDown();
                latchForMsg.await();
            } catch (InterruptedException e) {
                // ignore
            }
        });

        // finish processing 1
        log.debug("Releasing 1...");
        startLatch.await();
        msg1Lock.countDown();

        // make sure no offsets are committed
        verify(producerSpy, after(wait).never()).commitTransaction(); // todo smelly, change to event based - this is racey

        // finish 2
        msg2Lock.countDown();

        // make sure no offsets are committed
        verify(producerSpy, after(wait).never()).commitTransaction();

        // finish 0
        msg0Lock.countDown();

        // make sure offset 2, not 0 or 1 is committed
        verify(producerSpy, after(wait).times(1)).commitTransaction();
        var maps = producerSpy.consumerGroupOffsetsHistory();
        assertThat(maps).hasSize(1);
        OffsetAndMetadata offsets = maps.get(0).get(CONSUMER_GROUP_ID).get(toTP(firstRecord));
        assertThat(offsets.offset()).isEqualTo(2);

        // finish 3
        msg3Lock.countDown();

        // 3 committed
        verify(producerSpy, after(wait).times(2)).commitTransaction();
        maps = producerSpy.consumerGroupOffsetsHistory();
        assertThat(maps).hasSize(2);
        offsets = maps.get(1).get(CONSUMER_GROUP_ID).get(toTP(firstRecord));
        assertThat(offsets.offset()).isEqualTo(3);

        // finish 4,5
        msg4Lock.countDown();
        msg5Lock.countDown();

        // 5 committed
        verify(producerSpy, after(wait).times(3)).commitTransaction();
        maps = producerSpy.consumerGroupOffsetsHistory();
        assertThat(maps).hasSize(3);
        offsets = maps.get(2).get(CONSUMER_GROUP_ID).get(toTP(firstRecord));
        assertThat(offsets.offset()).isEqualTo(5);

        assertCommits(of(2, 3, 5));

        // close
        asyncConsumer.close();
    }

    @Test
    public void offsetCommitsAreIsolatedPerPartition() {
        // send three messages - 0,1, to one partition and 3,4 to another partition petitions
        consumer.addRecord(makeRecord(1, "0", "v2"));
        consumer.addRecord(makeRecord(1, "0", "v3"));

        var msg0Lock = new CountDownLatch(1);
        var msg1Lock = new CountDownLatch(1);
        var msg2Lock = new CountDownLatch(1);
        var msg3Lock = new CountDownLatch(1);

        List<CountDownLatch> locks = of(msg0Lock, msg1Lock, msg2Lock, msg3Lock);

        asyncConsumer.asyncVoidPoll((ignore) -> {
            int offset = (int) ignore.offset();
            CountDownLatch latchForMsg = locks.get(offset);
            try {
                latchForMsg.await();
            } catch (InterruptedException e) {
                // ignore
            }
        });

        // finish processing 1
        msg1Lock.countDown();

        // make sure no offsets are committed
        assertCommits(of());

        // finish 2
        msg2Lock.countDown();

        // make sure only 2 on it's partition of committed
        verify(producerSpy, after(wait).times(1)).commitTransaction();
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> maps = producerSpy.consumerGroupOffsetsHistory();
        assertCommits(of(2));

        // finish 0
        msg0Lock.countDown();

        // make sure offset 0 and 1 is committed
        verify(producerSpy, after(wait).times(2)).commitTransaction();
        assertCommits(of(2, 1));

        // finish 3
        msg3Lock.countDown();

        //
        verify(producerSpy, after(wait).times(3)).commitTransaction();
        assertCommits(of(2, 1, 3));
    }

    @Test
    @Disabled
    public void avro() {
        // send three messages - 0,1,2
        // finish processing 1
        // make sure no offsets are committed
        // finish 0
        // make sure offset 1, not 0 is committed
        assertThat(false).isTrue();
    }

    @Test
    @SneakyThrows
    public void controlFlowException() {
        when(consumerSpy.groupMetadata()).thenThrow(new RuntimeException("My fake control loop error"));
        var reentrantLock = new CountDownLatch(2);
        asyncConsumer.asyncVoidPoll((ignore) -> {
            reentrantLock.countDown();
            return;
        });
        reentrantLock.await(defaultTimeoutSeconds, SECONDS);
        asyncConsumer.waitForNoInFlight(defaultTimeout);
        Throwable throwable = catchThrowable(() -> {
            asyncConsumer.close();
        });
        log.trace("Caught error: ", throwable);
        assertThat(throwable).hasMessageContainingAll("Error", "poll", "thread", "fake control");
    }

    @Test
    @SneakyThrows
    public void testVoid() {
        int expected = 2;
        var reentrantLock = new CountDownLatch(expected);
        asyncConsumer.asyncVoidPoll((record) -> {
            myRecordProcessingAction.apply(record);
            reentrantLock.countDown();
        });
        reentrantLock.await(defaultTimeoutSeconds, SECONDS);
        asyncConsumer.close(defaultTimeout);
        verify(myRecordProcessingAction, times(expected)).apply(any());
        verify(producerSpy).commitTransaction();
        verify(producerSpy).sendOffsetsToTransaction(anyMap(), ArgumentMatchers.<ConsumerGroupMetadata>any());
    }

    @Test
    public void testStream() {
        var streamedResults = asyncConsumer.asyncPollAndStream((record) -> {
            Object result = Math.random();
            log.info("Consumed and a record ({}), and returning a derivative result to produce to output topic: {}", record, result);
            myRecordProcessingAction.apply(record);
            return Lists.list(result);
        });

        asyncConsumer.close(); // wait for everything to finish oustanding work

        verify(myRecordProcessingAction, times(2)).apply(any());

        var peekedStream = streamedResults.peek(x ->
        {
            log.info("streaming test {}", x.getLeft().value());
        });

        assertThat(peekedStream).hasSize(2);
    }

    @Test
    public void testConsumeAndProduce() {
        var stream = asyncConsumer.asyncPollAndProduce((record) -> {
            String apply = myRecordProcessingAction.apply(record);
            ProducerRecord<MyKey, MyInput> result = new ProducerRecord<>(OUTPUT_TOPIC, new MyKey("akey"), new MyInput(apply));
            log.info("Consumed and a record ({}), and returning a derivative result record to be produced: {}", record, result);
            List<ProducerRecord<MyKey, MyInput>> result1 = Lists.list(result);
            return result1;
        });

        asyncConsumer.close();

        verify(myRecordProcessingAction, times(2)).apply(any());

        var myResultStream = stream.peek(x -> {
            if (x != null) {
                ConsumerRecord<MyKey, MyInput> left = x.getLeft();
//                ProducerRecord<MyKey, MyInput> middle = x.getMiddle();
                AsyncConsumer.Tuple<ProducerRecord<MyKey, MyInput>, RecordMetadata> right = x.getRight();
                log.info("{}:{}:{}", left.key(), left.value(), right);
            } else {
                log.info("null");
            }
        });

        var collect = myResultStream.collect(Collectors.toList());

        assertThat(collect).hasSize(2);
    }

    @Test
    public void testFlatMapProduce() {
        var myResultStream = asyncConsumer.asyncPollAndStream((record) -> {
            String apply1 = myRecordProcessingAction.apply(record);
            String apply2 = myRecordProcessingAction.apply(record);
            List<String> list = Lists.list(apply1, apply2);
            return list;
        });

        asyncConsumer.close();

        verify(myRecordProcessingAction, times(4)).apply(any());

        assertThat(myResultStream).hasSize(4);
    }

    @Test
    public void testProducerStep() {
        ProducerRecord<MyKey, MyInput> outMsg = new ProducerRecord(OUTPUT_TOPIC, "");
        RecordMetadata prodResult = asyncConsumer.produceMessage(outMsg);
        assertThat(prodResult).isNotNull();

        List<ProducerRecord<MyKey, MyInput>> history = producer.history();
        assertThat(history).hasSize(1);
    }

    @Test
    @Disabled
    public void userSucceedsButProduceFails() {
    }

    @Test
    @Disabled
    public void poisonPillGoesToDeadLetterQueue() {
    }

    @Test
    @Disabled
    public void failingMessagesDontBreakCommitOrders() {
        assertThat(false).isTrue();
    }

    @Test
    @Disabled
    public void messagesCanBeProcessedOptionallyPartitionOffsetOrder() {
    }

    @Test
    @Disabled
    public void failingMessagesThatAreRetriedDontBreakProcessingOrders() {
        assertThat(false).isTrue();
    }

    @Test
    @Disabled
    public void ifTooManyMessagesAreInFlightDontPollBrokerForMore() {
    }

}
