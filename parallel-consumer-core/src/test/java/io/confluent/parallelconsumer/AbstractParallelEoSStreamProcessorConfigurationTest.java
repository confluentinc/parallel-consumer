package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
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
import java.util.List;

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

            List<WorkContainer<String, String>> normalWorkers = testInstance.handleStaleWork(workContainers);

            Assertions.assertEquals(normalWorkers.size(), 1);
            Assertions.assertEquals(testInstance.getMailBox().size(), 1);
        }
    }

    @Test
    void testHandleStaleWorkNoSplit() {
        List<WorkContainer<String, String>> workContainers = new ArrayList<>();

        workContainers.add(new WorkContainer<String, String>(0, new ConsumerRecord<>(topic, partition, 0, "test_k", "test_v1"), module));
        workContainers.add(new WorkContainer<String, String>(0, new ConsumerRecord<>(topic, partition, 1, "test_k", "test_v2"), module));

        try (final TestParallelEoSStreamProcessor<String, String> testInstance = new TestParallelEoSStreamProcessor<>(testOptions)) {
            testInstance.setWm(wm);

            List<WorkContainer<String, String>> normalWorkers = testInstance.handleStaleWork(workContainers);

            Assertions.assertEquals(normalWorkers.size(), 2);
            Assertions.assertEquals(testInstance.getMailBox().size(), 0);
        }
    }
}
