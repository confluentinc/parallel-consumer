package io.confluent.csid.asyncconsumer;

import ch.qos.logback.classic.Level;
import io.confluent.csid.utils.GeneralTestUtils;
import io.confluent.csid.utils.KafkaTestUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static io.confluent.csid.asyncconsumer.AsyncConsumerOptions.ProcessingOrder.*;
import static io.confluent.csid.utils.GeneralTestUtils.changeLogLevelTo;
import static io.confluent.csid.utils.GeneralTestUtils.time;
import static io.confluent.csid.utils.Range.range;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@Slf4j
public class VolumeTests extends AsyncConsumerTestBase {

    KafkaTestUtils ku = new KafkaTestUtils(consumerSpy);

    @SneakyThrows
    @Test
    public void load() {
        GeneralTestUtils.changeLogLevelTo(Level.OFF);

        setupClients();
        setupAsyncConsumerInstance(UNORDERED);

//        int quantityOfMessagesToProduce = 1_000_000;
        int quantityOfMessagesToProduce = 500;

        List<ConsumerRecord<String, String>> records = ku.generateRecords(quantityOfMessagesToProduce);
        ku.send(consumerSpy, records);

        CountDownLatch latch = new CountDownLatch(quantityOfMessagesToProduce);

        asyncConsumer.asyncPollAndProduce((rec) -> {
            ProducerRecord<String, String> mock = mock(ProducerRecord.class);
            return List.of(mock);
        }, (x) -> {
//            log.debug(x.toString());
            latch.countDown();
        });


        //
//        waitAtMost(defaultTimeout).until(() -> producerSpy.consumerGroupOffsetsHistory().size() > 0);
        latch.await(defaultTimeoutSeconds, SECONDS);
        asyncConsumer.waitForNoInFlight(defaultTimeout.multipliedBy(10));


        // assert quantity of produced messages
        List<ProducerRecord<String, String>> history = producerSpy.history();
        assertThat(history).hasSize(quantityOfMessagesToProduce);

        // assert order of produced messages


        // assert order of commits
//        int keySize = ku.getKeys().size();
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> commitHistory = producerSpy.consumerGroupOffsetsHistory();
        Map<String, Map<TopicPartition, OffsetAndMetadata>> mostRecent = commitHistory.get(commitHistory.size() - 1);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = mostRecent.get(CONSUMER_GROUP_ID);
        OffsetAndMetadata mostRecentTPCommit = topicPartitionOffsetAndMetadataMap.get(new TopicPartition(INPUT_TOPIC, 0));

        assertCommitsAlwaysIncrease();

        assertThat(mostRecentCommit(producerSpy)).isEqualTo(quantityOfMessagesToProduce - 1);

//        assertThat(false).isTrue();
//        Assert process ordering

        // Run the test of Run the test of the three different modes IKEA by topic by partition
    }


    private void assertCommitsAlwaysIncrease() {
        var map = new HashMap<TopicPartition, List<Long>>();
        for (var stringMapMap : producerSpy.consumerGroupOffsetsHistory()) {
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : stringMapMap.get(CONSUMER_GROUP_ID).entrySet()) {
                map.computeIfAbsent(entry.getKey(), (ignore) -> new ArrayList<>()).add(entry.getValue().offset());
            }
        }
        log.trace("Sorted offset commit history: {}", map);
        for (var entry : map.entrySet()) {
            var value = entry.getValue();
            long lastSeenOffset = value.get(0);
            for (var offset : value) {
                if (lastSeenOffset > offset)
                    throw new AssertionError("Offsets not in incrementing order: last seen: " + lastSeenOffset + " vs current: " + offset);
            }
        }
    }

    private long mostRecentCommit(MockProducer<?, ?> producerSpy) {
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> commitHistory = producerSpy.consumerGroupOffsetsHistory();
        Map<String, Map<TopicPartition, OffsetAndMetadata>> mostRecent = commitHistory.get(commitHistory.size() - 1);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = mostRecent.get(CONSUMER_GROUP_ID);
        OffsetAndMetadata mostRecentTPCommit = topicPartitionOffsetAndMetadataMap.get(new TopicPartition(INPUT_TOPIC, 0));

        return mostRecentTPCommit.offset();
    }

    @Test
//    @Disabled
    public void timingOfDifferentOrderingTypes() {
        var quantityOfMessagesToProduce = 100;
        var defaultNumKeys = 20;

//        setupClients();
        setupAsyncConsumerInstance(UNORDERED);
        log.debug("No order");
        Duration unordered = time(() -> testTiming(defaultNumKeys, quantityOfMessagesToProduce));

//        setupClients();
        setupAsyncConsumerInstance(KEY);
        for (var keySize : List.of(1, 2, 5, 10, 20, 50, 100, 1000)) {
//            setupClients();
            setupAsyncConsumerInstance(KEY);
            log.debug("By key, {} keys", keySize);
            Duration key = time(() -> testTiming(keySize, quantityOfMessagesToProduce));
        }

//        setupClients();
        setupAsyncConsumerInstance(PARTITION);
        log.debug("By partition");
        Duration partition = time(() -> testTiming(defaultNumKeys, quantityOfMessagesToProduce));

        assertThat(unordered).isLessThan(partition).as("No ordering constraint should be fastest");
    }

    private void testTiming(int numberOfKeys, int quantityOfMessagesToProduce) {
        List<Integer> keys = range(numberOfKeys).list();
        HashMap<Integer, List<ConsumerRecord<String, String>>> records = ku.generateRecords(keys, quantityOfMessagesToProduce);
        ku.send(consumerSpy, records);

        asyncConsumer.asyncPollAndProduce((rec) -> {
            int rangeOfTimeSimulatedProcessingTakesMs = 10;
            long sleepTime = (long) (Math.random() * rangeOfTimeSimulatedProcessingTakesMs);
            sleep(sleepTime);
            ProducerRecord<String, String> stub = new ProducerRecord<>(OUTPUT_TOPIC, "key", "Processing took: " + sleepTime);
            return List.of(stub);
        }, (x) -> {
//            log.debug(x.toString());
        });

        asyncConsumer.waitForNoInFlight(defaultTimeout.multipliedBy(10));

        List<ProducerRecord<String, String>> history = producerSpy.history();
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> maps = producerSpy.consumerGroupOffsetsHistory();

        assertThat(history).hasSize(quantityOfMessagesToProduce);
    }

    @SneakyThrows
    private void sleep(long ms) {
        log.trace("Sleeping for {}", ms);
        Thread.sleep(ms);
    }

}
