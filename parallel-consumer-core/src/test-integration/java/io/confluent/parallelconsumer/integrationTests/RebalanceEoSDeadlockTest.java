
/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests;

import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.internal.PCModule;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.REUSE_GROUP;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;

/**
 * Originally created to reproduce the bug #541 https://github.com/confluentinc/parallel-consumer/issues/541
 * <p>
 * This test reproduces the potential deadlock situation when a rebalance occurs
 * using EoS with transactional producer configuration.
 * The solution aims to avoid the deadlock by reordering the acquisition of locks
 * in the {@link io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor#onPartitionsRevoked(Collection)} method.
 *
 * @author Nacho Munoz
 */
@Slf4j
class RebalanceEoSDeadlockTest extends BrokerIntegrationTest<String, String> {

    private static final String PC_CONTROL = "pc-control";
    public static final String PC_BROKER_POLL = "pc-broker-poll";
    Consumer<String, String> consumer;
    Producer<String, String> producer;

    CountDownLatch rebalanceLatch;
    private long sleepTimeMs = 0L;

    ParallelEoSStreamProcessor<String, String> pc;

    {
        super.numPartitions = 2;
    }

    private String outputTopic;
    @BeforeEach
    void setup() {
        rebalanceLatch = new CountDownLatch(1);
        setupTopic();
        outputTopic = setupTopic("output-topic");
        producer = getKcu().createNewProducer(KafkaClientUtils.ProducerMode.TRANSACTIONAL);
        consumer = getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP);
        var pcOptions = ParallelConsumerOptions.<String,String>builder()
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .consumer(consumer)
                .produceLockAcquisitionTimeout(Duration.ofMinutes(2))
                .producer(producer)
                .ordering(PARTITION) // just so we dont need to use keys
                .build();

        pc = new ParallelEoSStreamProcessor<>(pcOptions, new PCModule<>(pcOptions)) {

            @Override
            protected void commitOffsetsThatAreReady() throws TimeoutException, InterruptedException {
                final var threadName = Thread.currentThread().getName();
                if (threadName.contains(PC_CONTROL)) {
                    log.info("Delaying pc-control thread {}ms to force the potential deadlock on rebalance", sleepTimeMs);
                    ThreadUtils.sleepQuietly(sleepTimeMs);
                }

                super.commitOffsetsThatAreReady();

                if (threadName.contains(PC_BROKER_POLL)) {
                    // onPartitionsRevoked managed to commit the offsets
                    rebalanceLatch.countDown();
                }
            }
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                super.onPartitionsRevoked(partitions);
            }
        };

        pc.subscribe(UniSets.of(topic));
    }

    @AfterEach
    void cleanup(){
        pc.close();
    }
    final static long SLEEP_TIME_MS = 3000L;
    @SneakyThrows
    @RepeatedTest(5)
    void noDeadlockOnRevoke() {
        this.sleepTimeMs = (long) (SLEEP_TIME_MS + (Math.random() * 1000));
        var numberOfRecordsToProduce = 100L;
        var count = new AtomicLong();

        getKcu().produceMessages(topic, numberOfRecordsToProduce);
        pc.setTimeBetweenCommits(ofSeconds(1));
        // consume some records
        pc.pollAndProduce((recordContexts) -> {
            count.getAndIncrement();
            log.debug("Processed record, count now {} - offset: {}", count, recordContexts.offset());
            return new ProducerRecord<>(outputTopic, recordContexts.key(), recordContexts.value());
        });

        await().timeout(Duration.ofSeconds(30)).untilAtomic(count,is(greaterThan(5L)));
        log.debug("Records are getting consumed");

        // cause rebalance
        final Duration newPollTimeout = Duration.ofSeconds(5);
        log.debug("Creating new consumer in same group and subscribing to same topic set with a no record timeout of {}, expect this phase to take entire timeout...", newPollTimeout);
        try (var newConsumer = getKcu().createNewConsumer(REUSE_GROUP)) {
            newConsumer.subscribe(UniLists.of(topic));
            newConsumer.poll(newPollTimeout);

            if (!rebalanceLatch.await(30, TimeUnit.SECONDS)) {
                Assertions.fail("Rebalance did not finished");
            }
            log.debug("Test finished");
        }
    }
}
