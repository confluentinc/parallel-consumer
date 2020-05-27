package io.confluent.csid.asyncconsumer;

import lombok.Data;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.csid.utils.KafkaUtils.toTP;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class AsyncConsumerTestBase {

    public static final String INPUT_TOPIC = "input";
    public static final String OUTPUT_TOPIC = "output";
    public static final String CONSUMER_GROUP_ID = "my-group";

    MockConsumer<MyKey, MyInput> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    MockProducer<MyKey, MyInput> producer = new MockProducer<>(true, null, null); // TODO do async testing

    protected MockConsumer<MyKey, MyInput> consumerSpy;
    protected MockProducer<MyKey, MyInput> producerSpy;

    protected AsyncConsumer<MyKey, MyInput> asyncConsumer;

    protected int defaultTimeoutSeconds = 10;

    protected Duration defaultTimeout = ofSeconds(defaultTimeoutSeconds);
    protected Duration infiniteTimeout = Duration.ofMinutes(20);

    AsyncConsumerTest.MyAction myRecordProcessingAction;

    ConsumerRecord<MyKey, MyInput> firstRecord;

    int offset = 0;

    @Data
    public class MyKey {
        private final String data;
    }

    @Data
    public static class MyInput {
        private final String data;
    }

    @BeforeEach
    public void setupAsyncConsumerTestBase() {
        producerSpy = spy(producer);
        consumerSpy = spy(consumer);
        when(consumerSpy.groupMetadata()).thenReturn(new ConsumerGroupMetadata(CONSUMER_GROUP_ID)); // todo fix AK mock consumer

        myRecordProcessingAction = mock(AsyncConsumerTest.MyAction.class);
        TopicPartition tp1 = new TopicPartition(INPUT_TOPIC, 1);
        TopicPartition tp0 = new TopicPartition(INPUT_TOPIC, 0);
        consumer.assign(Lists.list(tp0, tp1));

        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(tp0, 0L);
        beginningOffsets.put(tp1, 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        firstRecord = makeRecord("0", "v0");
        consumer.addRecord(firstRecord);
        consumer.addRecord(makeRecord("0", "v1"));
    }

    // TODO
//    @AfterEach
//    public void closeConsumer(){
//
//    }

    protected ConsumerRecord<MyKey, MyInput> makeRecord(String key, String value) {
        return makeRecord(0, key, value);
    }

    protected ConsumerRecord<MyKey, MyInput> makeRecord(int part, String key, String value) {
        ConsumerRecord<MyKey, MyInput> stringStringConsumerRecord = new ConsumerRecord<>(INPUT_TOPIC, part, offset, new MyKey(key), new MyInput(value));
        offset++;
        return stringStringConsumerRecord;
    }

    protected void assertCommits(List<Integer> integers) {
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> maps = producerSpy.consumerGroupOffsetsHistory();
        assertThat(maps).hasSameSizeAs(integers);
        assertThat(maps).describedAs("Which offsets are committed and in the expected order")
                .flatExtracting(x ->
                {
                    // get all partitino offsets and flatten
                    var results = new ArrayList<Integer>();
                    var y = x.get(CONSUMER_GROUP_ID);
                    for (var z : y.entrySet()) {
                        results.add((int) z.getValue().offset());
                    }
//                    return (int) y
//                            .get(toTP(firstRecord))
//                            .offset();
                    return results;
                })
                .isEqualTo(integers);
    }

}
