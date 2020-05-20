package io.confluent.csid.asyncconsumer;

import lombok.Data;
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

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@Slf4j
public class AsyncConsumerTest {

    public static final String INPUT_TOPIC = "input";
    public static final String OUTPUT_TOPIC = "output";

    MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    MockProducer<String, String> producer = new MockProducer<>(true, null, null); // TODO do async testing

    MockConsumer<String, String> consumerSpy;
    MockProducer<String, String> producerSpy;

    AsyncConsumer<MyKey, MyInput> asyncConsumer;

    int defaultTimeoutSeconds = 1;

    @Data
    class MyKey {
        private final String data;
    }

    @Data
    static
    class MyInput {
        private final String data;
    }

    MyAction myRecordProcessingAction;

    @BeforeEach
    public void setup() {
        producerSpy = spy(producer);
        consumerSpy = spy(consumer);
        when(consumerSpy.groupMetadata()).thenReturn(new ConsumerGroupMetadata("my-group")); // todo fix AK mock consumer
        asyncConsumer = new AsyncConsumer(consumerSpy, producerSpy);
        asyncConsumer.setLongPollTimeout(ofMillis(100));
        asyncConsumer.setTimeBetweenCommits(ofMillis(100));

        myRecordProcessingAction = mock(MyAction.class);
        consumer.assign(Lists.list(new TopicPartition(INPUT_TOPIC, 0)));

        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition(INPUT_TOPIC, 0), 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        consumer.addRecord(makeRecord("0", "v0"));
        consumer.addRecord(makeRecord("0", "v1"));
    }

    int offset = 0;

    private ConsumerRecord<String, String> makeRecord(String key, String value) {
        ConsumerRecord<String, String> stringStringConsumerRecord = new ConsumerRecord<>(INPUT_TOPIC, 0, offset, key, value);
        offset++;
        return stringStringConsumerRecord;
    }

    static class MyAction implements Function<ConsumerRecord<MyKey, MyInput>, String> {

        @Override
        public String apply(ConsumerRecord<MyKey, MyInput> record) {
            log.info("User client function - consuming a record... {}", record.key());
            return "my-result";
        }
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
        asyncConsumer.waitForNoInflight(ofSeconds(50));
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
        asyncConsumer.close();
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
                ProducerRecord<MyKey, MyInput> middle = x.getMiddle();
                RecordMetadata right = x.getRight();
                log.info("{}:{}", left.key(), left.value());
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

        List<ProducerRecord<String, String>> history = producer.history();
        assertThat(history).hasSize(1);
    }

    @Test
    public void failingActionGetsRetried(){
    }

    @Test
    @Disabled
    public void poisonPillGoesToDeadLetterQueue(){
    }

    @Test
    @Disabled
    public void failingMessagesDontBreakCommitOrders(){}

    @Test
    @Disabled
    public void messagesCanBeProcessedOptionallyPartitionOffsetOrder(){}

    @Test
    @Disabled
    public void failingMessagesThatAreRetriedDontBreakProcessingOrders(){}

    @Test
    @Disabled
    public void ifTooManyMessagesAreInFlightDontPollBrokerForMore(){}



}
