
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.RESUE_GROUP;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.shaded.org.hamcrest.Matchers.equalTo;
import static org.testcontainers.shaded.org.hamcrest.Matchers.is;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
class RebalanceTest extends BrokerIntegrationTest<String, String> {

    public static final Duration INFINITE = Duration.ofDays(1);

    @SneakyThrows
    @Test
    void commitUponRevoke() {
        var recordsCount = 20;
        var count = new AtomicLong();

        //
        kcu.produceMessages(topic, recordsCount);

        // effectively disable commit
        pc.setTimeBetweenCommits(INFINITE);

        // consume all the messages
        pc.poll(recordContexts -> count.getAndIncrement());
        await().untilAtomic(count, is(equalTo((long) recordsCount)));

        // cause rebalance
        var newConsumer = kcu.createNewConsumer(RESUE_GROUP);
        newConsumer.subscribe(List.of(topic));
        ConsumerRecords<Object, Object> poll = newConsumer.poll(Duration.ofSeconds(5));

        // make sure only there are no duplicates
        assertThat(poll).hasCountEqualTo(0);
    }

}
