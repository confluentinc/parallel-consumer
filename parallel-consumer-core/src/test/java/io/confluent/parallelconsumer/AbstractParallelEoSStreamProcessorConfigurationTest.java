package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import io.confluent.parallelconsumer.internal.TestParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import io.confluent.parallelconsumer.state.ModelUtils;
import io.confluent.parallelconsumer.state.PartitionState;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.BatchStrategy.BATCH_BY_SHARD;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.BatchStrategy.BATCH_MULTIPLEX;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.BatchStrategy.SEQUENTIAL;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests to verify the protected and internal methods of
 * {@link io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor} work as expected.
 * <p>
 *
 * @author Jonathon Koyle
 */
@Slf4j
class AbstractParallelEoSStreamProcessorConfigurationTest {
    final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    final ParallelConsumerOptions<String, String> testOptions = ParallelConsumerOptions.<String, String>builder()
            .consumer(consumer)
            .build();

    ModelUtils mu = new ModelUtils();
    PartitionState<String, String> state;
    WorkManager<String, String> wm;

    String topic = "myTopic";
    int partition = 0;

    TopicPartition tp = new TopicPartition(topic, partition);
    PCModule module = new PCModuleTestEnv();

    @BeforeEach
    public void setup() {
        state = new PartitionState<>(0, mu.getModule(), tp, OffsetMapCodecManager.HighestOffsetAndIncompletes.of());
        wm = mu.getModule().workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
    }

    /**
     * Test that the {@link io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor#getQueueTargetLoaded}
     */
    @Test
    void queueTargetLoad() {
        final int batchSize = 10;
        final int concurrency = 2;
        final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        final ParallelConsumerOptions<String, String> testOptions = ParallelConsumerOptions.<String, String>builder()
                .batchSize(batchSize)
                .maxConcurrency(concurrency)
                .consumer(consumer)
                .build();
        try (final TestParallelEoSStreamProcessor<String, String> testInstance = new TestParallelEoSStreamProcessor<>(testOptions)) {
            final int defaultLoad = 2;
            final int expectedTargetLoad = batchSize * concurrency * defaultLoad;

            final int actualTargetLoad = testInstance.getTargetLoad();

            Assertions.assertEquals(expectedTargetLoad, actualTargetLoad);
        }
    }

    @Test
    void testHandleStaleWorkSplit() {
        List<WorkContainer<String, String>> workContainers = new ArrayList<>();

        workContainers.add(new WorkContainer<String, String>(0, new ConsumerRecord<>(topic, partition, 0, "test_k", "test_v1"), module));
        workContainers.add(new WorkContainer<String, String>(1, new ConsumerRecord<>(topic, partition, 1, "test_k", "test_v2"), module));

        try (final TestParallelEoSStreamProcessor<String, String> testInstance = new TestParallelEoSStreamProcessor<>(testOptions)) {
            testInstance.setWm(wm);
            Function<PollContextInternal<String, String>, List<String>> dummyFunction = (contextInternal) -> new ArrayList<>();
            Consumer<String> callback = (res) -> {
            };

            testInstance.runUserFunc(dummyFunction, callback, workContainers);


            Assertions.assertEquals(testInstance.getMailBoxSuccessCnt(), 1);
            Assertions.assertEquals(testInstance.getMailBoxFailedCnt(), 1);
        }
    }

    @Test
    void testHandleStaleWorkNoSplit() {
        List<WorkContainer<String, String>> workContainers = new ArrayList<>();

        workContainers.add(new WorkContainer<String, String>(0, new ConsumerRecord<>(topic, partition, 0, "test_k", "test_v1"), module));
        workContainers.add(new WorkContainer<String, String>(0, new ConsumerRecord<>(topic, partition, 1, "test_k", "test_v2"), module));

        try (final TestParallelEoSStreamProcessor<String, String> testInstance = new TestParallelEoSStreamProcessor<>(testOptions)) {
            testInstance.setWm(wm);
            Function<PollContextInternal<String, String>, List<String>> dummyFunction = (contextInternal) -> new ArrayList<>();
            Consumer<String> callback = (res) -> {
            };

            testInstance.runUserFunc(dummyFunction, callback, workContainers);


            Assertions.assertEquals(testInstance.getMailBoxSuccessCnt(), 2);
            Assertions.assertEquals(testInstance.getMailBoxFailedCnt(), 0);
        }
    }

    /** Confirms the default batch strategy remains sequential. */
    @Test
    void defaultBatchStrategyIsSequential() {
        assertThat(testOptions.getBatchStrategy()).isEqualTo(SEQUENTIAL);
    }

    /** Ensures batch size one always produces singleton batches. */
    @Test
    void batchSizeOneMakesAllStrategiesProduceSingletonBatches() {
        List<WorkContainer<String, String>> work = Arrays.asList(
                newWorkContainer(0, 0, "key-a"),
                newWorkContainer(1, 1, "key-b"),
                newWorkContainer(0, 2, "key-c")
        );

        for (ParallelConsumerOptions.BatchStrategy strategy : ParallelConsumerOptions.BatchStrategy.values()) {
            try (TestParallelEoSStreamProcessor<String, String> processor = newProcessor(UNORDERED, strategy, 1)) {
                var batches = processor.makeBatchesForTest(work);
                assertThat(batchSizes(batches)).as("batch sizes for %s", strategy).containsExactly(1, 1, 1);
            }
        }
    }

    /** Verifies unordered sequential and multiplex batching stay size-based. */
    @Test
    void unorderedSequentialAndMultiplexUseSizeBasedBatching() {
        List<WorkContainer<String, String>> work = Arrays.asList(
                newWorkContainer(0, 0, "key-a"),
                newWorkContainer(0, 1, "key-a"),
                newWorkContainer(1, 2, "key-b"),
                newWorkContainer(1, 3, "key-b")
        );

        for (ParallelConsumerOptions.BatchStrategy strategy : Arrays.asList(SEQUENTIAL, BATCH_MULTIPLEX)) {
            try (TestParallelEoSStreamProcessor<String, String> processor = newProcessor(UNORDERED, strategy, 3)) {
                var batches = processor.makeBatchesForTest(work);

                assertThat(batchSizes(batches)).as("batch sizes for %s", strategy).containsExactly(3, 1);
                assertThat(offsetsForBatch(batches.get(0))).containsExactly(0L, 1L, 2L);
                assertThat(partitionsForBatch(batches.get(0))).containsExactly(0, 0, 1);
            }
        }
    }

    /** Verifies unordered shard batching splits by topic-partition. */
    @Test
    void unorderedBatchByShardBatchesPerTopicPartition() {
        List<WorkContainer<String, String>> work = Arrays.asList(
                newWorkContainer(0, 0, "key-a"),
                newWorkContainer(0, 1, "key-b"),
                newWorkContainer(1, 2, "key-a"),
                newWorkContainer(1, 3, "key-b")
        );

        try (TestParallelEoSStreamProcessor<String, String> processor = newProcessor(UNORDERED, BATCH_BY_SHARD, 10)) {
            var batches = processor.makeBatchesForTest(work);

            assertThat(batchSizes(batches)).containsExactly(2, 2);
            assertThat(offsetsForBatch(batches.get(0))).containsExactly(0L, 1L);
            assertThat(partitionsForBatch(batches.get(0))).containsExactly(0, 0);
            assertThat(offsetsForBatch(batches.get(1))).containsExactly(2L, 3L);
            assertThat(partitionsForBatch(batches.get(1))).containsExactly(1, 1);
        }
    }

    /** Verifies key-ordered shard batching splits by key. */
    @Test
    void keyOrderedBatchByShardBatchesPerKey() {
        List<WorkContainer<String, String>> work = Arrays.asList(
                newWorkContainer(0, 0, "key-a"),
                newWorkContainer(0, 1, "key-a"),
                newWorkContainer(0, 2, "key-b"),
                newWorkContainer(0, 3, "key-b")
        );

        try (TestParallelEoSStreamProcessor<String, String> processor = newProcessor(KEY, BATCH_BY_SHARD, 10)) {
            var batches = processor.makeBatchesForTest(work);

            assertThat(batchSizes(batches)).containsExactly(2, 2);
            assertThat(offsetsForBatch(batches.get(0))).containsExactly(0L, 1L);
            assertThat(keysForBatch(batches.get(0))).containsExactly("key-a", "key-a");
            assertThat(offsetsForBatch(batches.get(1))).containsExactly(2L, 3L);
            assertThat(keysForBatch(batches.get(1))).containsExactly("key-b", "key-b");
        }
    }

    /** Verifies partition-ordered multiplex batching can mix partitions. */
    @Test
    void partitionOrderedMultiplexCanMixPartitionsInOneBatch() {
        List<WorkContainer<String, String>> work = Arrays.asList(
                newWorkContainer(0, 0, "key-a"),
                newWorkContainer(1, 1, "key-b"),
                newWorkContainer(2, 2, "key-c")
        );

        try (TestParallelEoSStreamProcessor<String, String> processor = newProcessor(PARTITION, BATCH_MULTIPLEX, 3)) {
            var batches = processor.makeBatchesForTest(work);

            assertThat(batchSizes(batches)).containsExactly(3);
            assertThat(partitionsForBatch(batches.get(0))).containsExactly(0, 1, 2);
        }
    }

    /** Verifies partition-ordered shard batching keeps partitions separate. */
    @Test
    void partitionOrderedBatchByShardSeparatesPartitions() {
        List<WorkContainer<String, String>> work = Arrays.asList(
                newWorkContainer(0, 0, "key-a"),
                newWorkContainer(0, 1, "key-b"),
                newWorkContainer(1, 2, "key-c"),
                newWorkContainer(1, 3, "key-d")
        );

        try (TestParallelEoSStreamProcessor<String, String> processor = newProcessor(PARTITION, BATCH_BY_SHARD, 10)) {
            var batches = processor.makeBatchesForTest(work);

            assertThat(batchSizes(batches)).containsExactly(2, 2);
            assertThat(partitionsForBatch(batches.get(0))).containsExactly(0, 0);
            assertThat(partitionsForBatch(batches.get(1))).containsExactly(1, 1);
        }
    }

    private TestParallelEoSStreamProcessor<String, String> newProcessor(ParallelConsumerOptions.ProcessingOrder order,
                                                                        ParallelConsumerOptions.BatchStrategy strategy,
                                                                        int batchSize) {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .ordering(order)
                .batchStrategy(strategy)
                .batchSize(batchSize)
                .build();
        return new TestParallelEoSStreamProcessor<>(options);
    }

    private WorkContainer<String, String> newWorkContainer(int partition, long offset, String key) {
        return new WorkContainer<>(0, new ConsumerRecord<>(topic, partition, offset, key, "value-" + offset), module);
    }

    private List<Integer> batchSizes(List<List<WorkContainer<String, String>>> batches) {
        List<Integer> sizes = new ArrayList<>();
        batches.forEach(batch -> sizes.add(batch.size()));
        return sizes;
    }

    private List<Long> offsetsForBatch(List<WorkContainer<String, String>> batch) {
        List<Long> offsets = new ArrayList<>();
        batch.forEach(workContainer -> offsets.add(workContainer.offset()));
        return offsets;
    }

    private List<Integer> partitionsForBatch(List<WorkContainer<String, String>> batch) {
        List<Integer> partitions = new ArrayList<>();
        batch.forEach(workContainer -> partitions.add(workContainer.getTopicPartition().partition()));
        return partitions;
    }

    private List<String> keysForBatch(List<WorkContainer<String, String>> batch) {
        List<String> keys = new ArrayList<>();
        batch.forEach(workContainer -> keys.add(workContainer.getCr().key()));
        return keys;
    }
}
