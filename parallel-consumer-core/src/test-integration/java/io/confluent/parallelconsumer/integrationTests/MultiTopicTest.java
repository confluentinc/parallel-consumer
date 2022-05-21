package io.confluent.parallelconsumer.integrationTests;

import io.confluent.parallelconsumer.ManagedTruth;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
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
class MultiTopicTest extends BrokerIntegrationTest<String, String> {

    @Test
    void multiTopic() {
        int numTopics = 3;
        List<NewTopic> topics = getKcu().createTopics(numTopics);
        int recordsPerTopic = 333;
        topics.forEach(topic -> sendMessages(topic, recordsPerTopic));

        var pc = getKcu().buildPc(KEY);

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


    @SneakyThrows
    private void sendMessages(NewTopic topic, int recordsPerTopic) {
        getKcu().produceMessages(topic.name(), recordsPerTopic);
    }

    private void assertCommit(NewTopic topic, int recordsPerTopic) {
        ManagedTruth.assertThat(getKcu().getLastConsumerConstructed())
                .hasCommittedToPartition(topic)
                .offset(recordsPerTopic);
    }

}
