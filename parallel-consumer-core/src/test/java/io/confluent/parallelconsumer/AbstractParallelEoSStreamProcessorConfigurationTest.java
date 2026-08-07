package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.DynamicLoadFactor;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import io.confluent.parallelconsumer.internal.TestParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import io.confluent.parallelconsumer.state.ModelUtils;
import io.confluent.parallelconsumer.state.PartitionState;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    PCModuleTestEnv module;
    ModelUtils mu;
    PartitionState<String, String> state;
    WorkManager<String, String> wm;

    String topic = "myTopic";
    int partition = 0;

    TopicPartition tp = new TopicPartition(topic, partition);

    @BeforeEach
    public void setup() {
        module = new PCModuleTestEnv(testOptions);
        mu = new ModelUtils(module);

        wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
        state = new PartitionState<>(0, module, tp, OffsetMapCodecManager.HighestOffsetAndIncompletes.of());
    }

    /**
     * Test that the
     * {@link io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor#getQueueTargetLoaded}
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

    private boolean isVirtualThreadsSupported() {
        try {
            Class.forName("java.lang.Thread").getMethod("ofVirtual");
            java.util.concurrent.Executors.class.getMethod("newThreadPerTaskExecutor", java.util.concurrent.ThreadFactory.class);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isCurrentThreadVirtual() {
        try {
            Method m = Thread.class.getMethod("isVirtual");
            return (boolean) m.invoke(Thread.currentThread());
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    void testVirtualThreadBackpressureStability() {
        Assumptions.assumeTrue(isVirtualThreadsSupported(), "JDK 21+ required for Virtual Threads");

        var vtOptions = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .useVirtualThreads(true)
                .maxConcurrency(100)
                .build();

        try (var testInstance = new TestParallelEoSStreamProcessor<String, String>(vtOptions) {
            @Override
            public void checkPipelinePressure() {
                super.checkPipelinePressure();
            }

            public DynamicLoadFactor getLoadFactor() {
                return this.dynamicExtraLoadFactor;
            }
        }) {
            // Record the initial load factor
            int initialLoadFactor = testInstance.getLoadFactor().getCurrentFactor();

            testInstance.checkPipelinePressure();
            int loadFactorAfterCheck = testInstance.getLoadFactor().getCurrentFactor();

            // The load factor should remain stable when we're at or near max concurrency
            assertThat(loadFactorAfterCheck)
                    .as("Load factor should not increase excessively when near max concurrency")
                    .isLessThanOrEqualTo(initialLoadFactor + 1); // Allow for at most 1 step increase

            // Verify the system is stable by checking multiple times
            for (int i = 0; i < 5; i++) {
                testInstance.checkPipelinePressure();
            }

            int finalLoadFactor = testInstance.getLoadFactor().getCurrentFactor();
            assertThat(finalLoadFactor)
                    .as("Load factor should remain reasonable after multiple pressure checks")
                    .isLessThan(DynamicLoadFactor.DEFAULT_MAX_LOADING_FACTOR);
        }
    }

    @Test
    void testVirtualThreadActivation() throws Exception {
        Assumptions.assumeTrue(isVirtualThreadsSupported(), "JDK 21+ required for Virtual Threads");

        int maxConcurrency = 10;
        var vtOptions = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .useVirtualThreads(true)
                .maxConcurrency(maxConcurrency)
                .build();

        try (var testInstance = new TestParallelEoSStreamProcessor<>(vtOptions) {
            @Override
            public Supplier<ExecutorService> getWorkerThreadPool() {
                return super.getWorkerThreadPool();
            }
        }) {
            assertThat(testInstance.getWorkerThreadPool().get()).isNotNull();

            AtomicInteger virtualThreadCount = new AtomicInteger();
            CountDownLatch latch = new CountDownLatch(1);

            Future<?> future = testInstance.getWorkerThreadPool().get().submit(() -> {
                if (isCurrentThreadVirtual()) {
                    virtualThreadCount.incrementAndGet();
                }
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            latch.countDown();
            future.get();

            // Ensure VT is used in the test
            assertThat(virtualThreadCount.get()).isEqualTo(1);
        }
    }

    @Test
    @SneakyThrows
    void testVirtualThreadCleanShutdown() {
        Assumptions.assumeTrue(isVirtualThreadsSupported(), "JDK 21+ required for Virtual Threads");

        var vtOptions = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .useVirtualThreads(true)
                .build();

        try (var testInstance = new TestParallelEoSStreamProcessor<>(vtOptions) {
            @Override
            public Supplier<ExecutorService> getWorkerThreadPool() {
                return super.getWorkerThreadPool();
            }
        }) {
            testInstance.getWorkerThreadPool().get().submit(() -> {
                try {
                    Thread.sleep(Duration.ofMinutes(1).toMillis());
                } catch (InterruptedException ignored) {
                }
            });

            Assertions.assertTimeout(Duration.ofSeconds(5), () -> {
                testInstance.close();
            }, "Virtual thread pool should shutdown cleanly and interrupt running tasks");
        }
    }
}