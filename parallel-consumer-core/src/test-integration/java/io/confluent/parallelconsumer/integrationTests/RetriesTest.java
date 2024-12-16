
/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2024 Confluent, Inc.
 */

import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.PCRetriableException;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.shaded.org.hamcrest.Matchers.equalTo;
import static org.testcontainers.shaded.org.hamcrest.Matchers.is;

@Slf4j
public class RetriesTest extends BrokerIntegrationTest<String, String> {

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
                .maxConcurrency(100)
                .defaultMessageRetryDelay(Duration.ofMillis(200))
                .messageBufferSize(10000)
                .build();

        pc = new ParallelEoSStreamProcessor<>(pcOpts);

        pc.subscribe(UniSets.of(topic));
    }

    @Test
    @SneakyThrows
    void awaitingWorkContainersSizeDoesNotExceedNumberOfFailedContainersInRetryLoop() {
        String throwExceptionFlag = "throw";
        var latch = new CountDownLatch(1);

        getKcu().produceMessagesWithThrowHeader(topic, 4000);
        AtomicInteger count = new AtomicInteger(0);
        AtomicBoolean throwOnHeader = new AtomicBoolean(true);
        pc.poll((context) -> {
            ThreadUtils.sleepQuietly(7); //not multiple of retry delay or vice versa
            if (throwOnHeader.get()) {
                if (context.getSingleConsumerRecord().headers().lastHeader(throwExceptionFlag) != null) {
                    throw new PCRetriableException("THROW_EXCEPTION_FLAG_HAPPENED");
                }
            }
            count.incrementAndGet();
        });
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(100)).until(count::get, is(equalTo(2000))); //wait for all successful messages to complete
        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicBoolean checking = new AtomicBoolean(true);
        new Thread(() -> {
            try {
                while (checking.get()) {
                    long countAwaiting = pc.getWm().getSm().getNumberOfWorkQueuedInShardsAwaitingSelection();
                    log.debug("NumberOfWorkQueuedInShardsAwaitingSelection : {}", countAwaiting);
                    assertThat(countAwaiting).isBetween(0L, 2000L);
                    ThreadUtils.sleepQuietly(RandomUtils.nextInt(1, 10));
                }
            } catch (AssertionError e) {
                failed.set(true);
                throw e;
            } finally {
                latch.countDown();
            }
        }).start();
        ThreadUtils.sleepQuietly(3000);
        throwOnHeader.set(false);
        ThreadUtils.sleepQuietly(2000);
        checking.set(false);
        latch.await();
        assertThat(failed.get()).isFalse();
    }
}