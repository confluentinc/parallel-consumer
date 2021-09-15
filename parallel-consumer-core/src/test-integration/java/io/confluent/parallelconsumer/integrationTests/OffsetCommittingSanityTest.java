package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import static io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase.defaultTimeoutSeconds;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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
        String topicNameForTest = setupTopic("foo");
        List<Long> producedOffsets = new ArrayList<>();
        List<Long> consumedOffsets = new ArrayList<>();

        KafkaProducer<String, String> kafkaProducer = kcu.createNewProducer(false);

        // offset 0
        sendCheckClose(topicNameForTest, producedOffsets, consumedOffsets, kafkaProducer, "key-0", "value-0", true);

        assertCommittedOffset(topicNameForTest, 1);

        // offset 1
        sendCheckClose(topicNameForTest, producedOffsets, consumedOffsets, kafkaProducer, "key-1", "value-1", true);

        assertCommittedOffset(topicNameForTest, 2);

        // sanity
        assertThat(producedOffsets).containsExactly(0L, 1L);
        assertThat(consumedOffsets).containsExactly(0L, 1L);
    }

    @Test
    void shouldNotSkipAnyMessagesOnRestartAsDescribed() throws Exception {
        String topicNameForTest = setupTopic("foo");
        List<Long> producedOffsets = new ArrayList<>();
        List<Long> consumedOffsets = new ArrayList<>();

        KafkaProducer<String, String> kafkaProducer = kcu.createNewProducer(false);

        // offset 0
        sendCheckClose(topicNameForTest, producedOffsets, consumedOffsets, kafkaProducer, "key-0", "value-0", true);

        assertCommittedOffset(topicNameForTest, 1);

        // offset 1
        sendCheckClose(topicNameForTest, producedOffsets, consumedOffsets, kafkaProducer, "key-1", "value-1", false);

        // offset 2
        sendCheckClose(topicNameForTest, producedOffsets, consumedOffsets, kafkaProducer, "key-2", "value-2", true);
    }

    /**
     * Sends a record Runs PC, conditionally checking that PC consumes that sent message Closes PC, waiting for it to
     * drain
     */
    private void sendCheckClose(String topic,
                                List<Long> producedOffsets,
                                List<Long> consumedOffsets,
                                KafkaProducer<String, String> kafkaProducer,
                                String key, String val,
                                boolean check) throws Exception {
        var record = new ProducerRecord<>(topic, key, val);
        Future<RecordMetadata> send = kafkaProducer.send(record);
        long offset = send.get().offset();
        producedOffsets.add(offset);

        //
        var newConsumer = kcu.createNewConsumer(false);
        var pc = createParallelConsumer(topic, newConsumer);

        //
        pc.poll(consumerRecord -> consumedOffsets.add(consumerRecord.offset()));

        //
        if (check) {
            assertThatCode(() -> {
                waitAtMost(ofSeconds(defaultTimeoutSeconds)).alias("all produced messages consumed")
                        .untilAsserted(
                                () -> assertThat(consumedOffsets).isEqualTo(producedOffsets));
            }).doesNotThrowAnyException(); // wait for no concurrent exceptions
        } else {
            Thread.sleep(2000);
        }
        pc.closeDrainFirst();
    }

    /**
     * Starts a new consumer for the topic, and checking it's committed offsets that it's sent to start from
     */
    private void assertCommittedOffset(String topicNameForTest, long expectedOffset) {
        // assert committed offset
        var newConsumer = kcu.createNewConsumer(false);
        newConsumer.subscribe(UniSets.of(topicNameForTest));
        // increased poll timeout to allow for delay under load during parallel test execution
        newConsumer.poll(ofSeconds(5));
        Set<TopicPartition> assignment = newConsumer.assignment();
        Truth.assertWithMessage("Should be assigned some partitions").that(assignment).isNotEmpty();

        //
        Map<TopicPartition, OffsetAndMetadata> committed = newConsumer.committed(assignment);
        Truth.assertThat(committed).isNotEmpty();

        //
        TopicPartition tp = new TopicPartition(topicNameForTest, 0);
        OffsetAndMetadata offsetAndMetadata = committed.get(tp);
        assertThat(offsetAndMetadata).as("Should have commit history for this partition {}", tp).isNotNull();

        //
        long offset = offsetAndMetadata.offset();
        assertThat(offset).isEqualTo(expectedOffset);
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
