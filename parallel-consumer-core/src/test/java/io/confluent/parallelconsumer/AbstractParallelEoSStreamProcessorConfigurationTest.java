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

    @Test
    void testPartitions() {
        List<String> emptyList = new ArrayList<>();
        List<String> list1 = Arrays.asList("1", "2", "3", "4");
        List<String> list2 = Arrays.asList("1", "2", "3", "4", "5", "6", "7");

        final TestParallelEoSStreamProcessor<String, String> testInstance = new TestParallelEoSStreamProcessor<>(testOptions);
        List<List<String>> emptyres = testInstance.partitionList(emptyList, 2);
        List<List<String>> res1 = testInstance.partitionList(list1, 2);
        List<List<String>> res2 = testInstance.partitionList(list2, 3);
        Assertions.assertEquals(emptyres.size(), 1);
        Assertions.assertTrue(emptyres.get(0).isEmpty());
        Assertions.assertEquals(res1.size(), 2);
        Assertions.assertEquals(res1.get(0), Arrays.asList("1", "2"));
        Assertions.assertEquals(res1.get(1), Arrays.asList("3", "4"));
        Assertions.assertEquals(res2.size(), 3);
        Assertions.assertEquals(res2.get(0), Arrays.asList("1", "2", "3"));
        Assertions.assertEquals(res2.get(1), Arrays.asList("4", "5", "6"));
        Assertions.assertEquals(res2.get(2), Arrays.asList("7"));

    }
}
