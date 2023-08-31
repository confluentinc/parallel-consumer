package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.internal.PCModule;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.NEW_GROUP;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption.REUSE_GROUP;
import static io.confluent.parallelconsumer.metrics.PCMetricsDef.PC_INSTANCE_TAG;
import static io.confluent.parallelconsumer.metrics.PCMetricsDef.PROCESSED_RECORDS;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;

@Slf4j
class MultiInstanceMetricsTest extends BrokerIntegrationTest<String, String> {
    {
        super.numPartitions = 2;
    }

    SimpleMeterRegistry simpleMeterRegistry;
    private String outputTopic;

    @BeforeEach
    void setup() {
        setupTopic();
        simpleMeterRegistry = new SimpleMeterRegistry();
    }

    @AfterEach
    void cleanup() {
        simpleMeterRegistry.close();
    }


    @SneakyThrows
    @Test
    void twoInstancePCMetricsRecordedIndependently() {

        var numberOfRecordsToProduce = 100L;

        getKcu().produceMessages(topic, numberOfRecordsToProduce);

        String pcInstance1Tag = UUID.randomUUID().toString();

        var pcOptions = getOptions(pcInstance1Tag, NEW_GROUP);

        ParallelEoSStreamProcessor<String, String> pc = new ParallelEoSStreamProcessor<>(pcOptions, new PCModule<>(pcOptions));
        pc.subscribe(UniSets.of(topic));

        String pcInstance2Tag = UUID.randomUUID().toString();
        pcOptions = getOptions(pcInstance2Tag, REUSE_GROUP);

        ParallelEoSStreamProcessor<String, String> pc2 = new ParallelEoSStreamProcessor<>(pcOptions, new PCModule<>(pcOptions));
        pc2.subscribe(UniSets.of(topic));

        AtomicInteger pc1Counter = new AtomicInteger();
        AtomicInteger pc2Counter = new AtomicInteger();
        pc.poll(record -> pc1Counter.incrementAndGet());

        pc2.poll(record -> pc2Counter.incrementAndGet());


        await().timeout(Duration.ofSeconds(30)).until(() -> pc1Counter.get() + pc2Counter.get() == numberOfRecordsToProduce);
        assertThat(Search.in(simpleMeterRegistry).tag(PC_INSTANCE_TAG, pcInstance1Tag).name(PROCESSED_RECORDS.getName()).counter().count()).isEqualTo(pc1Counter.get());
        assertThat(Search.in(simpleMeterRegistry).tag(PC_INSTANCE_TAG, pcInstance2Tag).name(PROCESSED_RECORDS.getName()).counter().count()).isEqualTo(pc2Counter.get());
        pc.close();
        pc2.close();
    }

    @SneakyThrows
    @Test
    void sameRegistryCanBeReusedAfterPcInstanceClosed() {

        var numberOfRecordsToProduce = 20;

        getKcu().produceMessages(topic, numberOfRecordsToProduce);


        var pcOptions = getOptions(null, NEW_GROUP);

        ParallelEoSStreamProcessor<String, String> pc = new ParallelEoSStreamProcessor<>(pcOptions, new PCModule<>(pcOptions));
        pc.subscribe(UniSets.of(topic));


        AtomicInteger pc1Counter = new AtomicInteger();
        pc.poll(record -> pc1Counter.incrementAndGet());


        await().timeout(Duration.ofSeconds(30)).until(() -> pc1Counter.get() == numberOfRecordsToProduce);
        assertThat(Search.in(simpleMeterRegistry).name(PROCESSED_RECORDS.getName()).counters().stream().mapToDouble(Counter::count).sum()).isEqualTo(pc1Counter.get());
        pc.close();

        getKcu().produceMessages(topic, numberOfRecordsToProduce);

        pcOptions = getOptions(null, NEW_GROUP);

        ParallelEoSStreamProcessor<String, String> pc2 = new ParallelEoSStreamProcessor<>(pcOptions, new PCModule<>(pcOptions));
        pc2.subscribe(UniSets.of(topic));
        AtomicInteger pc2Counter = new AtomicInteger();

        pc2.poll(record -> pc2Counter.incrementAndGet());
        await().timeout(Duration.ofSeconds(30)).until(() -> pc2Counter.get() == numberOfRecordsToProduce * 2);
        assertThat(Search.in(simpleMeterRegistry).name(PROCESSED_RECORDS.getName()).counters().stream().mapToDouble(Counter::count).sum()).isEqualTo(pc2Counter.get());

        pc2.close();
    }

    @SneakyThrows
    @Test
    void allMetersRemovedFromRegistryOnClose() {
        var numberOfRecordsToProduce = 10L;
        getKcu().produceMessages(topic, numberOfRecordsToProduce);
        String pcInstance1Tag = UUID.randomUUID().toString();

        var pcOptions = getOptions(pcInstance1Tag, NEW_GROUP);

        ParallelEoSStreamProcessor<String, String> pc = new ParallelEoSStreamProcessor<>(pcOptions, new PCModule<>(pcOptions));
        pc.subscribe(UniSets.of(topic));
        AtomicInteger pc1Counter = new AtomicInteger();
        pc.poll(record -> pc1Counter.incrementAndGet());
        await().timeout(Duration.ofSeconds(30)).until(() -> pc1Counter.get() == numberOfRecordsToProduce);
        assertThat(Search.in(simpleMeterRegistry).tag(PC_INSTANCE_TAG, pcInstance1Tag).name(PROCESSED_RECORDS.getName()).counters().stream().mapToDouble(Counter::count).sum()).isEqualTo(pc1Counter.get());
        pc.close();
        assertThat(simpleMeterRegistry.getMeters().size()).isEqualTo(0);
    }


    ParallelConsumerOptions<String, String> getOptions(String pcInstanceTag, KafkaClientUtils.GroupOption consumerGroupOption) {
        return ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .consumer(getKcu().createNewConsumer(consumerGroupOption))
                .meterRegistry(simpleMeterRegistry)
                .pcInstanceTag(pcInstanceTag)
                .ordering(PARTITION) // just so we dont need to use keys
                .build();

    }
}
