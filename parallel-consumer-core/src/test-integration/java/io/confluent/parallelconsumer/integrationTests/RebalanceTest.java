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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

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
    }

    @AfterEach
    void cleanup() {
        pc.close();
    }

    private ParallelEoSStreamProcessor<String, String> setupPC() {
        return setupPC(null);
    }

    private ParallelEoSStreamProcessor<String, String> setupPC(Function<ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String>, ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String>> optionsCustomizer) {
        ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder =
                ParallelConsumerOptions.<String, String>builder()
                        .consumer(consumer)
                        .ordering(PARTITION);
        if (optionsCustomizer != null) {
            optionsBuilder = optionsCustomizer.apply(optionsBuilder);
        }

        return new ParallelEoSStreamProcessor<>(optionsBuilder.build());
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
        pc = setupPC();
        pc.subscribe(UniSets.of(topic));

        //
        getKcu().produceMessages(topic, numberOfRecordsToProduce);

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
        var newConsumer = getKcu().createNewConsumer(REUSE_GROUP);
        newConsumer.subscribe(UniLists.of(topic));
        log.debug("Polling with new group member for records with timeout {}...", newPollTimeout);
        ConsumerRecords<Object, Object> newConsumersPollResult = newConsumer.poll(newPollTimeout);
        log.debug("Poll complete");

        // make sure only there are no duplicates
        assertThat(newConsumersPollResult).hasCountEqualTo(0);
        log.debug("Test finished");
    }

    static Stream<Arguments> rebalanceTestCommitModes() {
        return Stream.of(
                Arguments.of("Consumer Async", ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS, null),
                Arguments.of("Consumer Sync", ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC, null),
                Arguments.of("Consumer Async + Producer Non-transactional", ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS, KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL),
                Arguments.of("Consumer Sync + Producer Non-transactional", ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS, KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL),
                Arguments.of("Transactional Producer", ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER, KafkaClientUtils.ProducerMode.TRANSACTIONAL)
        );
    }

    @SneakyThrows
    @MethodSource("rebalanceTestCommitModes")
    @ParameterizedTest(name = "[{index}] - {0}")
    /**
     * Tests that re-balance completes on partition revocation in supported commit modes and consumer / producer combinations.
     * Issue was raised when producer was set, but not used - https://github.com/confluentinc/parallel-consumer/issues/637
     * bug in transactional producer commit synchronisation with re-balance triggered commit was not caught by existing tests.
     */
    void rebalanceCompletesForCommitModeVariations(String testName, ParallelConsumerOptions.CommitMode commitMode, KafkaClientUtils.ProducerMode producerMode) {
        var numberOfRecordsToProduce = 20L;
        var count = new AtomicLong();
        pc = setupPC(builder -> {
            builder = builder.commitMode(commitMode);
            if (producerMode != null) {
                builder = builder.producer(getKcu().createNewProducer(producerMode));
            }
            return builder;
        });
        pc.subscribe(UniSets.of(topic));
        //
        getKcu().produceMessages(topic, numberOfRecordsToProduce);

        // consume all the messages
        pc.poll(recordContexts -> {
            count.getAndIncrement();
            log.debug("PC1 - Processed record, count now {} - offset: {}", count, recordContexts.offset());
        });
        await().untilAtomic(count, is(equalTo(numberOfRecordsToProduce)));
        log.debug("All records consumed");
        Consumer<String, String> newConsumer = getKcu().createNewConsumer(REUSE_GROUP);

        ParallelEoSStreamProcessor<String, String> pc2 = setupPC(builder -> builder.consumer(newConsumer));
        pc2.subscribe(UniSets.of(topic));

        // cause rebalance
        pc2.poll(recordContexts -> {
            count.getAndIncrement();
            log.debug("PC2 - Processed record, count now {} - offset: {}", count, recordContexts.offset());
        });

        await().untilAsserted(() -> assertThat(pc2).getNumberOfAssignedPartitions().isEqualTo(1));
        getKcu().produceMessages(topic, numberOfRecordsToProduce);

        await().untilAtomic(count, is(equalTo(numberOfRecordsToProduce * 2)));
        pc.closeDrainFirst();
        await().untilAsserted(() -> assertThat(pc2).getNumberOfAssignedPartitions().isEqualTo(2));

        getKcu().produceMessages(topic, numberOfRecordsToProduce);

        await().untilAtomic(count, is(equalTo(numberOfRecordsToProduce * 3)));
        pc2.closeDrainFirst();
        await().untilAsserted(() -> {
            assertThat(pc2).isClosedOrFailed();
            org.assertj.core.api.Assertions.assertThat(count).hasValue(numberOfRecordsToProduce * 3);
        });
        log.debug("Test finished");
    }

}
