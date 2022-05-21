package io.confluent.parallelconsumer.integrationTests;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

/**
 * Originally created to investigate issue report #184
 */
@Slf4j
class MultiTopicTest {
    @Test
    void multiTopic() {
        int numTopics = 3;
        List<TopicPartition> topics = createTopics(numTopics);
        int recordsPerTopic = 333;
        topics.forEach(topic -> sendMessages(topic, recordsPerTopic));
        ParallelEoSStreamProcessor pc;
        ParallelConsumerOptions.builder().ordering(KEY);
        AtomicInteger messageProcessedCount = new AtomicInteger();
        pc.poll(pollContext -> {
            log.debug(pollContext.toString());
            messageProcessedCount.incrementAndGet();
        });

        // processed
        int expectedMessagesCount = recordsPerTopic * numTopics;
        await().untilAtomic(messageProcessedCount, Matchers.is(equalTo(expectedMessagesCount)));

        // commits
        await().untilAsserted(() -> {
            topics.forEach(topic -> assertCommit(topic, recordsPerTopic));
        });
    }

    private void sendMessages(TopicPartition topic, int recordsPerTopic) {

    }

    private List<TopicPartition> createTopics(int numTopics) {

    }

    private void assertCommit(TopicPartition topic, int recordsPerTopic) {

    }
}
