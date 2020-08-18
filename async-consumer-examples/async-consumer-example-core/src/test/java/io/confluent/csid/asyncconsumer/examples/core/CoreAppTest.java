package io.confluent.csid.asyncconsumer.examples.core;

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
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.HashMap;

import static io.confluent.csid.utils.KafkaTestUtils.DEFAULT_GROUP_METADATA;
import static org.mockito.Mockito.when;

@Slf4j
public class CoreAppTest {

    TopicPartition tp = new TopicPartition(CoreApp.inputTopic, 0);

    @SneakyThrows
    @Test
    public void test() {
        log.info("Test start");
        CoreAppUnderTest coreApp = new CoreAppUnderTest();

        coreApp.run();

        coreApp.mockConsumer.addRecord(new ConsumerRecord(CoreApp.inputTopic, 0, 0, "a key 1", "a value"));
        coreApp.mockConsumer.addRecord(new ConsumerRecord(CoreApp.inputTopic, 0, 1, "a key 2", "a value"));
        coreApp.mockConsumer.addRecord(new ConsumerRecord(CoreApp.inputTopic, 0, 2, "a key 3", "a value"));

        Awaitility.await().pollInterval(Duration.ofSeconds(1)).untilAsserted(()->{
            Assertions.assertThat(coreApp.mockConsumer.position(tp)).isEqualTo(3);
        });

        coreApp.close();
    }

    class CoreAppUnderTest extends CoreApp {

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
            mockConsumer.assign(UniLists.of(tp));
        }
    }
}
