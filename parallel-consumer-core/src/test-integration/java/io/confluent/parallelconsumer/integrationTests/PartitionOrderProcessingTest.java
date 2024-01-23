package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniSets;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Tests around what should happen when rebalancing occurs
 *
 * @author Antony Stubbs
 */
@Slf4j
class PartitionOrderProcessingTest extends BrokerIntegrationTest<String, String> {

    Consumer<String, String> consumer;

    ParallelEoSStreamProcessor<String, String> pc;

    {
        super.numPartitions = 5;
    }

    // todo refactor move up
    @BeforeEach
    void setup() {
        setupTopic();
        consumer = getKcu().createNewConsumer(true, consumerProps());
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
                        .ordering(PARTITION)
                        .maxConcurrency(5);
        if (optionsCustomizer != null) {
            optionsBuilder = optionsCustomizer.apply(optionsBuilder);
        }

        return new ParallelEoSStreamProcessor<>(optionsBuilder.build());
    }

    /**
     * Check that all partitions are processed in parallel and not starved when Consumer options for max partition fetch
     * size and ParallelConsumer buffer size are tuned. Increasing ParallelConsumer buffer size improves parallel
     * processing load over the {@link #allPartitionsAreNotProcessedInParallel()} scenario due to having enough buffer
     * to fit 10 x polls so allows underlying Consumer to fetch from each partition at least 2 times before
     * back-pressure control pauses polling.
     */
    @SneakyThrows
    @Test
    void allPartitionsAreProcessedInParallel() {
        var numberOfRecordsToProduce = 10000L;
        Map<Integer, AtomicInteger> partitionCounts = new HashMap<>();
        IntStream.range(0, 5).forEach(part -> partitionCounts.put(part, new AtomicInteger(0)));
        pc = setupPC(options -> options.messageBufferSize(5000)); // Increasing message buffer to 10 * max partition fetch - to make sure mix of data from all partitions is available for processing
        pc.subscribe(UniSets.of(topic));

        //
        getKcu().produceMessages(topic, numberOfRecordsToProduce);

        // consume all the messages
        pc.poll(recordContexts -> {
            partitionCounts.get(recordContexts.getSingleConsumerRecord().partition()).getAndIncrement();
            ThreadUtils.sleepQuietly(10); // introduce a bit of processing delay - to make sure polling backpressure kicks in.
        });
        await().until(() -> partitionCounts.values().stream().mapToInt(AtomicInteger::get).sum() > 500); // wait until we process some messages to get the counts in.
        Assertions.assertTrue(partitionCounts.values().stream().allMatch(v -> v.get() > 0), "Expect all partitions to have some messages processed, actual partitionCounts:" + partitionCounts);

    }

    /**
     * Negative test of the {@link #allPartitionsAreProcessedInParallel} confirming that without tuning buffer size and
     * max partition fetch - processing threads are easily starved in Partition ordering mode.
     * <p>
     * This is due to default buffer size is small (max concurrency (5) * batchSize(1) * loadFactor (2 to 100) - gives
     * buffer of 10 to 500 max.
     * <p>
     * With reduced partition fetch size of 50KB and ~100 byte messages - we poll 500 messages per partition. But even
     * reducing partition fetch size still starves our processing threads as buffer is so small and we have pre-loaded
     * topics with data to simulate higher incoming rate than processing rate.
     */
    @SneakyThrows
    @Test
    void allPartitionsAreNotProcessedInParallel() {
        var numberOfRecordsToProduce = 10000L;
        Map<Integer, AtomicInteger> partitionCounts = new HashMap<>();
        IntStream.range(0, 5).forEach(part -> partitionCounts.put(part, new AtomicInteger(0)));
        pc = setupPC();
        pc.subscribe(UniSets.of(topic));

        //
        getKcu().produceMessages(topic, numberOfRecordsToProduce);

        // consume all the messages
        pc.poll(recordContexts -> {
            partitionCounts.get(recordContexts.getSingleConsumerRecord().partition()).getAndIncrement();
            ThreadUtils.sleepQuietly(10); // introduce a bit of processing delay - to make sure polling backpressure kicks in.
        });
        await().until(() -> partitionCounts.values().stream().mapToInt(AtomicInteger::get).sum() > 500); // wait until we process some messages to get the counts in.
        Assertions.assertFalse(partitionCounts.values().stream().allMatch(v -> v.get() > 0), "Expect some processing thread starving and not all partition counts to have some messages processed, actual partitionCounts:" + partitionCounts);
    }

    //Tune consumer for smaller message polls both per partition and max poll records.
    private Properties consumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 500 * 100); //500 * ~100 byte message
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500); //default
        return props;
    }

}
