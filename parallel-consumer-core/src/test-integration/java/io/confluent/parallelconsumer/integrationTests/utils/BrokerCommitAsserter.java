package io.confluent.parallelconsumer.integrationTests.utils;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import pl.tlinkowski.unij.api.UniSets;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.NEW_GROUP;
import static java.time.Duration.ofSeconds;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Central point for asserting commits in integration tests against the brokers.
 *
 * @author Antony Stubbs
 */
@RequiredArgsConstructor
public class BrokerCommitAsserter {

    @NonNull
    @Getter(AccessLevel.PRIVATE)
    private final KafkaClientUtils kcu;

    @NonNull
    @Getter(AccessLevel.PRIVATE)
    private final String defaultTopic;

    public void assertConsumedOffset(int target) {
        try (var assertConsumer = getKcu().createNewConsumer(NEW_GROUP)) {
            assertConsumer.subscribe(UniSets.of(getDefaultTopic()));

            await().untilAsserted(() -> {
                var poll = assertConsumer.poll(ofSeconds(1));
                assertThat(poll).hasHeadOffsetAnyTopicPartition(target);
            });
        }
    }

}
