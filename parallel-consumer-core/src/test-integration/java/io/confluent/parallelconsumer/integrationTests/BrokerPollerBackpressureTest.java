
/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests;

import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@Slf4j
public class BrokerPollerBackpressureTest extends BrokerIntegrationTest<String, String> {

    Consumer<String, String> consumer;

    ParallelConsumerOptions<String, String> pcOpts;
    ParallelEoSStreamProcessor<String, String> pc;

    @BeforeEach
    void setUp() {
        setupTopic();
        consumer = getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP);

        pcOpts = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .ordering(KEY)
                .maxConcurrency(10)
                .messageBufferSize(150)
                .build();

        pc = new ParallelEoSStreamProcessor<>(pcOpts);

        pc.subscribe(UniSets.of(topic));
    }

    @Test
    @SneakyThrows
    void brokerPollPausedWithEmptyShardsButHighInFlight() {
        var messageProcessingLatch = new CountDownLatch(1);
        assertThat(pc.getPausedPartitionSize()).isEqualTo(0); // should be polling initially
        getKcu().produceMessages(topic, 200);
        AtomicInteger count = new AtomicInteger(0);
        pc.poll((context) -> {
            try {
                count.incrementAndGet();
                messageProcessingLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(200)).until(() -> pc.getWm().getNumberOfWorkQueuedInShardsAwaitingSelection() == 0); //wait for all successful messages to complete
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> assertThat(pc.getPausedPartitionSize()).isEqualTo(1)); //polling should be paused even though shards size is 0;
        //give it a second to make sure it does not get resumed again
        ThreadUtils.sleepQuietly(1000);
        assertThat(pc.getPausedPartitionSize()).isEqualTo(1); // should be polling still paused
        messageProcessingLatch.countDown();
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> {
            assertThat(count.get()).isEqualTo(200);
            assertThat(pc.getWm().getNumberRecordsOutForProcessing()).isEqualTo(0);
        }); //wait for all messages to complete and verify no messages are out for processing anymore.
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> assertThat(pc.getPausedPartitionSize()).isEqualTo(0)); //should resume now that messages have been processed and inflight is 0.
        getKcu().produceMessages(topic, 10); //send 10 more - just to make sure PC is actually polling for msgs
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> assertThat(count.get()).isEqualTo(210)); //should process the 10 new messages
    }

    @Test
    @SneakyThrows
    void brokerPollPausedWithHighNumberInShardsButLowInFlight() {
        var messageProcessingLatch = new CountDownLatch(1);
        assertThat(pc.getPausedPartitionSize()).isEqualTo(0); // should be polling initially
        getKcu().produceMessages(topic, 200, 5);
        AtomicInteger count = new AtomicInteger(0);
        pc.poll((context) -> {
            try {
                count.incrementAndGet();
                messageProcessingLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(200)).until(() -> pc.getWm().getNumberOfWorkQueuedInShardsAwaitingSelection() == 195); //wait for all processing threads to be blocked
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> assertThat(pc.getPausedPartitionSize()).isEqualTo(1)); //polling should be paused with shards size being above buffer size and inflight being low;
        //give it a second to make sure it does not get resumed again
        ThreadUtils.sleepQuietly(1000);
        assertThat(pc.getPausedPartitionSize()).isEqualTo(1); // should be polling still paused
        messageProcessingLatch.countDown();
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> {
            assertThat(count.get()).isEqualTo(200);
            assertThat(pc.getWm().getNumberRecordsOutForProcessing()).isEqualTo(0);
        }); //wait for all messages to complete and verify no messages are out for processing anymore.
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> assertThat(pc.getPausedPartitionSize()).isEqualTo(0)); //should resume now that messages have been processed and inflight is 0.
        getKcu().produceMessages(topic, 10); //send 10 more - just to make sure PC is actually polling for msgs
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> assertThat(count.get()).isEqualTo(210)); //should process the 10 new messages
    }
}