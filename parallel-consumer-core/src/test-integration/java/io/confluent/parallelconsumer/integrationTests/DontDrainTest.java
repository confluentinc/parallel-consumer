package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

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
import java.util.concurrent.atomic.AtomicLong;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.shaded.org.hamcrest.Matchers.equalTo;
import static org.testcontainers.shaded.org.hamcrest.Matchers.is;

@Slf4j
public class DontDrainTest extends BrokerIntegrationTest<String, String> {

    Consumer<String, String> consumer;

    ParallelConsumerOptions<String, String> pcOpts;
    ParallelEoSStreamProcessor<String, String> pc;

    @BeforeEach
    void setUp() {
        setupTopic();
        consumer = getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP);

        pcOpts = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .ordering(PARTITION)
                .build();

        pc = new ParallelEoSStreamProcessor<>(pcOpts);

        pc.subscribe(UniSets.of(topic));
    }

    @Test
    @SneakyThrows
    void stopPollingAfterShutdownWhenStateIsSetToDontDrain() {
        // 1 in process + 2 waiting in shard queue
        var recordsToProduce = 3L;
        var recordsToProduceAfterClose = 10L;

        var count = new AtomicLong();
        var latch = new CountDownLatch(1);

        getKcu().produceMessages(topic, recordsToProduce);
        pc.poll(recordContexts -> {
            count.getAndIncrement();
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            log.debug("Processed record, count now {} - offset: {}", count, recordContexts.offset());
        });
        await().untilAtomic(count, is(equalTo(1L)));

        new Thread(() -> pc.closeDontDrainFirst(Duration.ofSeconds(30))).start();
        sleep(2000);

        getKcu().produceMessages(topic, recordsToProduceAfterClose);
        sleep(5000);

        latch.countDown();

        await().until(() -> pc.isClosedOrFailed()
                || count.get() == recordsToProduce + recordsToProduceAfterClose);
        //we only expect one here because we are shutting down without draining the records in the queue.
        assertEquals(1, count.get());
        log.debug("Test finished");
    }
}