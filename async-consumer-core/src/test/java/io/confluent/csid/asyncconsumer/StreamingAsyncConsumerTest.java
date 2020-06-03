package io.confluent.csid.asyncconsumer;

import io.confluent.csid.asyncconsumer.AsyncConsumer.ConsumeProduceResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

// TODO this class shouldn't have access to the non streaming async consumer - refactor out another super class layer
@Slf4j
public class StreamingAsyncConsumerTest extends AsyncConsumerTestBase {

    StreamingAsyncConsumer<MyKey, MyInput> streaming;

    @BeforeEach
    public void setupAsyncConsumerTest() {
        AsyncConsumerOptions options = AsyncConsumerOptions.builder().build();
        streaming = new StreamingAsyncConsumer<>(consumerSpy, producerSpy, options);
        streaming.setLongPollTimeout(ofMillis(100));
        streaming.setTimeBetweenCommits(ofMillis(100));
    }

    @Test
    public void testStream() {
        Stream<ConsumeProduceResult<MyKey, MyInput, MyKey, MyInput>> streamedResults = streaming.asyncPollProduceAndStream((record) -> {
//            Object result = Math.random();
            ProducerRecord mock = mock(ProducerRecord.class);
            log.info("Consumed and a record ({}), and returning a derivative result to produce to output topic: {}", record, mock);
            myRecordProcessingAction.apply(record);
            return Lists.list(mock);
        });

        streaming.close(); // wait for everything to finish outstanding work

        verify(myRecordProcessingAction, times(2)).apply(any());

        Stream<ConsumeProduceResult<MyKey, MyInput, MyKey, MyInput>> peekedStream = streamedResults.peek(x ->
        {
            log.info("streaming test {}", x.getIn().value());
        });

        assertThat(peekedStream).hasSize(2);
    }

    @Test
    public void testConsumeAndProduce() {
        var stream = streaming.asyncPollProduceAndStream((record) -> {
            String apply = myRecordProcessingAction.apply(record);
            ProducerRecord<MyKey, MyInput> result = new ProducerRecord<>(OUTPUT_TOPIC, new MyKey("akey"), new MyInput(apply));
            log.info("Consumed and a record ({}), and returning a derivative result record to be produced: {}", record, result);
            List<ProducerRecord<MyKey, MyInput>> result1 = Lists.list(result);
            return result1;
        });

        streaming.close();

        verify(myRecordProcessingAction, times(2)).apply(any());

        var myResultStream = stream.peek(x -> {
            if (x != null) {
                ConsumerRecord<MyKey, MyInput> left = x.getIn();
                log.info("{}:{}:{}:{}", left.key(), left.value(), x.getOut(), x.getMeta());
            } else {
                log.info("null");
            }
        });

        var collect = myResultStream.collect(Collectors.toList());

        assertThat(collect).hasSize(2);
    }


    @Test
    public void testFlatMapProduce() {
        var myResultStream = streaming.asyncPollProduceAndStream((record) -> {
            String apply1 = myRecordProcessingAction.apply(record);
            String apply2 = myRecordProcessingAction.apply(record);

            var list = Lists.list(
                    new ProducerRecord<>(OUTPUT_TOPIC, new MyKey("key"), new MyInput(apply1)),
                    new ProducerRecord<>(OUTPUT_TOPIC, new MyKey("key"), new MyInput(apply2)));
            return list;
        });

        streaming.close();

        verify(myRecordProcessingAction, times(4)).apply(any());

        assertThat(myResultStream).hasSize(4);
    }

}
