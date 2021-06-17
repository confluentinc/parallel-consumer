package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase.defaultTimeoutSeconds;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * Test offset restoring from boundary conditions, i.e. when no offset data is encoded in metadata
 * <p>
 * Reproduces issue 62: https://github.com/confluentinc/parallel-consumer/issues/62
 *
 * @see io.confluent.parallelconsumer.ParallelEoSStreamProcessorTest#closeOpenBoundaryCommits
 */
@Slf4j
class OffsetCommittingSanityTest extends BrokerIntegrationTest<String, String> {

    @Test
    void shouldNotSkipAnyMessagesOnRestartRoot() throws Exception {
        setupTopic("foo");
        List<Long> producedOffsets = new ArrayList<>();
        List<Long> consumedOffsets = new ArrayList<>();

        var kafkaProducer = kcu.createNewProducer(false);

        // offset 0
        sendCheckClose(producedOffsets, consumedOffsets, kafkaProducer, "key-0", "value-0", true);

        assertCommittedOffset(1);

        // offset 1
        sendCheckClose(producedOffsets, consumedOffsets, kafkaProducer, "key-1", "value-1", true);

        assertCommittedOffset(2);

        // sanity
        assertThat(producedOffsets).containsExactly(0L, 1L);
        assertThat(consumedOffsets).containsExactly(0L, 1L);
    }

    @Test
    void shouldNotSkipAnyMessagesOnRestartAsDescribed() throws Exception {
        setupTopic("foo");
        List<Long> producedOffsets = new ArrayList<>();
        List<Long> consumedOffsets = new ArrayList<>();

        var kafkaProducer = kcu.createNewProducer(false);

        // offset 0
        sendCheckClose(producedOffsets, consumedOffsets, kafkaProducer, "key-0", "value-0", true);

        assertCommittedOffset(1);

        // offset 1
        sendCheckClose(producedOffsets, consumedOffsets, kafkaProducer, "key-1", "value-1", false);

        // offset 2
        sendCheckClose(producedOffsets, consumedOffsets, kafkaProducer, "key-2", "value-2", true);
    }

    private void sendCheckClose(List<Long> producedOffsets,
                                List<Long> consumedOffsets,
                                KafkaProducer<Object, Object> kafkaProducer,
                                String key, String val,
                                boolean check) throws Exception {
        producedOffsets.add(kafkaProducer.send(new ProducerRecord<>(topic, key, val)).get().offset());
        var newConsumer = kcu.createNewConsumer(false);
        var pc = createParallelConsumer(topic, newConsumer);
        pc.poll(consumerRecord -> consumedOffsets.add(consumerRecord.offset()));
        if (check) {
            waitAtMost(ofSeconds(defaultTimeoutSeconds)).alias("all produced messages consumed")
                    .untilAsserted(() -> assertThat(consumedOffsets).isEqualTo(producedOffsets));
        } else {
            Thread.sleep(2000);
        }
        pc.closeDrainFirst();
    }

    private void assertCommittedOffset(long expectedOffset) {
        // assert committed offset
        var newConsumer = kcu.createNewConsumer(false);
        newConsumer.subscribe(UniSets.of(topic));
        newConsumer.poll(ofSeconds(1));
        Map<TopicPartition, OffsetAndMetadata> committed = newConsumer.committed(newConsumer.assignment());
        assertThat(committed.get(new TopicPartition(topic, 0)).offset()).isEqualTo(expectedOffset);
        newConsumer.close();
    }

    private ParallelEoSStreamProcessor<String, String> createParallelConsumer(String topicName, Consumer consumer) {
        ParallelEoSStreamProcessor<String, String> pc = new ParallelEoSStreamProcessor<>(ParallelConsumerOptions.builder()
                .consumer(consumer)
                .build()
        );
        pc.subscribe(UniLists.of(topicName));
        return pc;
    }

}
