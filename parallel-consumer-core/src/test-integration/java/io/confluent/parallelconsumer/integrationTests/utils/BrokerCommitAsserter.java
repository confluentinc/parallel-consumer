package io.confluent.parallelconsumer.integrationTests.utils;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import pl.tlinkowski.unij.api.UniSets;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static java.time.Duration.ofSeconds;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Central point for asserting commits in integration tests against the brokers.
 *
 * @author Antony Stubbs
 */
@Slf4j
@RequiredArgsConstructor
public class BrokerCommitAsserter {

    @NonNull
    @Getter(AccessLevel.PRIVATE)
    private final String defaultTopic;

    @NonNull
    private final KafkaConsumer<?, ?> assertConsumer;

    public void assertConsumedOffset(int target) {
        assertConsumedOffset(getDefaultTopic(), target);
    }

    public void assertConsumedOffset(String topic, int target) {
        log.debug("Asserting against topic: {}, expecting to consume at least offset {}", topic, target);
        assertConsumer.subscribe(UniSets.of(topic));

        await().untilAsserted(() -> {
            var poll = assertConsumer.poll(ofSeconds(1));
            log.debug("Polled {} records, looking for at least offset {}", poll.count(), target);
            assertThat(poll).hasHeadOffsetAnyTopicPartition(target);
        });
    }

}
