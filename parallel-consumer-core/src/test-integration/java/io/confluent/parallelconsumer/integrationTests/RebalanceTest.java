
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.RESUE_GROUP;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.shaded.org.hamcrest.Matchers.equalTo;
import static org.testcontainers.shaded.org.hamcrest.Matchers.is;

/**
 * Tests around what should happen when rebalancing occurs
 *
 * @author Antony Stubbs
 */
class RebalanceTest extends BrokerIntegrationTest<String, String> {


    Consumer<String, String> consumer;

    ParallelEoSStreamProcessor<String, String> pc;

    public static final Duration INFINITE = Duration.ofDays(1);

    {
        super.numPartitions = 2;
    }

    @BeforeEach
    void setup() {
        setupTopic();
        consumer = getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP);

        pc = new ParallelEoSStreamProcessor<>(ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .ordering(PARTITION) // just so we dont need to use keys
                .build());

        pc.subscribe(UniSets.of(topic));
    }

    /**
     * Checks that when a rebalance happens, a final commit is done first for revoked partitions (that will be assigned
     * to new consumers), so that the new consumer doesn't reprocess records that are already complete.
     */
    @SneakyThrows
    @Test
    void commitUponRevoke() {
        var recordsCount = 20L;
        var count = new AtomicLong();

        //
        kcu.produceMessages(topic, recordsCount);

        // effectively disable commit
        pc.setTimeBetweenCommits(INFINITE);

        // consume all the messages
        pc.poll(recordContexts -> count.getAndIncrement());
        await().untilAtomic(count, is(equalTo(recordsCount)));

        // cause rebalance
        var newConsumer = kcu.createNewConsumer(RESUE_GROUP);
        newConsumer.subscribe(List.of(topic));
        ConsumerRecords<Object, Object> poll = newConsumer.poll(Duration.ofSeconds(5));

        // make sure only there are no duplicates
        assertThat(poll).hasCountEqualTo(0);
    }

}
