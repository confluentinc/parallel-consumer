package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.REUSE_GROUP;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.shaded.org.hamcrest.Matchers.equalTo;
import static org.testcontainers.shaded.org.hamcrest.Matchers.is;

/**
 * Tests around what should happen when rebalancing occurs
 *
 * @author Antony Stubbs
 */
@Slf4j
class RebalanceTest extends BrokerIntegrationTest<String, String> {

    Consumer<String, String> consumer;

    ParallelEoSStreamProcessor<String, String> pc;

    public static final Duration INFINITE = Duration.ofDays(1);

    {
        super.numPartitions = 2;
    }

    // todo refactor move up
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
        var numberOfRecordsToProduce = 20L;
        var count = new AtomicLong();

        //
        kcu.produceMessages(topic, numberOfRecordsToProduce);

        // effectively disable commit
        pc.setTimeBetweenCommits(INFINITE);

        // consume all the messages
        pc.poll(recordContexts -> {
            count.getAndIncrement();
            log.debug("Processed record, count now {} - offset: {}", count, recordContexts.offset());
        });
        await().untilAtomic(count, is(equalTo(numberOfRecordsToProduce)));
        log.debug("All records consumed");

        // cause rebalance
        final Duration newPollTimeout = Duration.ofSeconds(5);
        log.debug("Creating new consumer in same group and subscribing to same topic set with a no record timeout of {}, expect this phase to take entire timeout...", newPollTimeout);
        var newConsumer = kcu.createNewConsumer(REUSE_GROUP);
        newConsumer.subscribe(UniLists.of(topic));
        log.debug("Polling with new group member for records with timeout {}...", newPollTimeout);
        ConsumerRecords<Object, Object> newConsumersPollResult = newConsumer.poll(newPollTimeout);
        log.debug("Poll complete");

        // make sure only there are no duplicates
        assertThat(newConsumersPollResult).hasCountEqualTo(0);
        log.debug("Test finished");
    }

}
