package io.confluent.csid.asyncconsumer.examples.vertx;

import io.confluent.csid.utils.LongPollingMockConsumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;

import static io.confluent.csid.utils.KafkaTestUtils.DEFAULT_GROUP_METADATA;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.when;

@Slf4j
public class VertxAppTest {

    TopicPartition tp = new TopicPartition(VertxApp.inputTopic, 0);

    @SneakyThrows
    @Test
    public void test() {
        log.info("Test start");
        VertxAppAppUnderTest coreApp = new VertxAppAppUnderTest();

        coreApp.run();

        coreApp.mockConsumer.addRecord(new ConsumerRecord(VertxApp.inputTopic, 0, 0, "a key 1", "a value"));
        coreApp.mockConsumer.addRecord(new ConsumerRecord(VertxApp.inputTopic, 0, 1, "a key 2", "a value"));
        coreApp.mockConsumer.addRecord(new ConsumerRecord(VertxApp.inputTopic, 0, 2, "a key 3", "a value"));

        Awaitility.await().pollInterval(Duration.ofSeconds(1)).untilAsserted(()->{
            Assertions.assertThat(coreApp.mockConsumer.position(tp)).isEqualTo(3);
        });

        assertThatExceptionOfType(ConditionTimeoutException.class)
                .as("no server to receive request, should timeout trying to close. Could also setup wire mock...")
                .isThrownBy(coreApp::close)
                .withMessageContainingAll("Condition", "lambda", "fulfilled", "Waiting", "records", "flight");
    }

    class VertxAppAppUnderTest extends VertxApp {

        LongPollingMockConsumer<String, String> mockConsumer = Mockito.spy(new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST));

        @Override
        Consumer<String, String> getKafkaConsumer() {
            HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
            beginningOffsets.put(tp, 0L);
            mockConsumer.updateBeginningOffsets(beginningOffsets);
            when(mockConsumer.groupMetadata()).thenReturn(DEFAULT_GROUP_METADATA); // todo fix AK mock consumer
            return mockConsumer;
        }

        @Override
        Producer<String, String> getKafkaProducer() {
            return new MockProducer<>();
        }

        @Override
        void setupSubscription(Consumer<String, String> kafkaConsumer) {
            mockConsumer.assign(List.of(tp));
        }
    }
}
