
/*-
 * Copyright (C) 2020 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests;

import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Testcontainers;
import pl.tlinkowski.unij.api.UniLists;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static io.confluent.csid.utils.GeneralTestUtils.time;
import static io.confluent.csid.utils.Range.range;
import static java.time.Duration.*;
import static me.tongfei.progressbar.ProgressBar.wrap;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@Slf4j
public class LoadTest extends DbTest {

    //    static int total = 8_000_0;
//    static int total = 4_000_00;
//    static int total = 4_000_0;
    static int total = 4_000;
//    static int total = 8;

    @SneakyThrows
    public void setupTestData() {
        setupTopic();

        publishMessages(total / 100, total, topic);
    }

    @SneakyThrows
    @Test
    void timedNormalKafkaConsumerTest() {
        setupTestData();
        // subscribe in advance, it can be a few seconds
        kcu.consumer.subscribe(UniLists.of(topic));

        readRecordsPlainConsumer(total, topic);
    }

    @SneakyThrows
    @Test
    void asyncConsumeAndProcess() {
        setupTestData();

        // subscribe in advance, it can be a few seconds
        kcu.consumer.subscribe(UniLists.of(topic));

        //
        ParallelConsumerOptions options = ParallelConsumerOptions.builder().ordering(ParallelConsumerOptions.ProcessingOrder.KEY).build();
        KafkaConsumer<String, String> newConsumer = kcu.createNewConsumer();
        newConsumer.subscribe(Pattern.compile(topic));
        var async = new ParallelEoSStreamProcessor<>(newConsumer, kcu.createNewProducer(true), options);
        AtomicInteger msgCount = new AtomicInteger(0);

        ProgressBar pb = new ProgressBarBuilder()
                .setInitialMax(total).showSpeed().setTaskName("Read async").setUnit("msg", 1)
                .build();

        try (pb) {
            async.register(r -> {
                // message processing function
                int simulatedCPUMessageProcessingDelay = nextInt(0, 5); // random delay between 0,5
                try {
                    Thread.sleep(simulatedCPUMessageProcessingDelay); // simulate json parsing overhead and network calls
                } catch (Exception ignore) {
                }
                // save to db
                savePayload(r.key(), r.value());
                //
                msgCount.getAndIncrement();
            });

            // keep checking how many message's we've processed
            await().atMost(ofSeconds(60)).until(() -> {
                // log.debug("msg count: {}", msgCount.get());
                pb.stepTo(msgCount.get());
                return msgCount.get() >= total;
            });
        }
        async.close();
    }

    private void readRecordsPlainConsumer(int total, String topic) {
        // read
        log.info("Starting to read back");
        final List<ConsumerRecord<String, String>> allRecords = Lists.newArrayList();
        time(() -> {
            ProgressBar pb = new ProgressBarBuilder()
                    .setInitialMax(total).showSpeed().setTaskName("Read").setUnit("msg", 1)
                    .build();

            try (pb) {
                await().atMost(ofSeconds(60)).untilAsserted(() -> {
                    ConsumerRecords<String, String> poll = kcu.consumer.poll(ofMillis(5000));
                    Iterable<ConsumerRecord<String, String>> records = poll.records(topic);
                    records.forEach(x -> {
                        // log.trace(x.toString());
                        savePayload(x.key(), x.value());
                        // log.debug(testDataEbean.toString());
                    });

                    ArrayList<ConsumerRecord<String, String>> c = Lists.newArrayList(records);
                    log.info("Got {} messages", c.size());
                    allRecords.addAll(c);
                    pb.stepTo(allRecords.size());

                    assertThat(allRecords).hasSize(total); // awaitility#untilAsserted
                });
            }
        });

        assertThat(allRecords).hasSize(total);
    }

    @SneakyThrows
    private void publishMessages(int keyRange, int total, String topic) {

        // produce data
        var keys = range(keyRange).list();
        var integers = Lists.newArrayList(IntStream.range(0, total).iterator());

        // publish
        var futureMetadataResultsFromPublishing = new LinkedList<Future<RecordMetadata>>();
        log.info("Start publishing...");
        time(() -> {
            for (var x : wrap(integers, "Publishing async")) {
                String key = keys.get(RandomUtils.nextInt(0, keys.size())).toString();
                int messageSizeInBytes = 500;
                String value = RandomStringUtils.randomAlphabetic(messageSizeInBytes);
                var producerRecord = new ProducerRecord<>(topic, key, value);
                try {
                    var meta = kcu.producer.send(producerRecord);
                    futureMetadataResultsFromPublishing.add(meta);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // joining
        Set<Integer> usedPartitions = new HashSet<>();
        for (var meta : wrap(futureMetadataResultsFromPublishing, "Joining")) {
            RecordMetadata recordMetadata = meta.get();
            int partition = recordMetadata.partition();
            usedPartitions.add(partition);
        }
        // has a certain chance of passing, if number of messages is ~large compared to numPartitions
        if (numPartitions > 100_000) {
            assertThat(usedPartitions.stream().distinct()).as("All partitions are made use of").hasSize(numPartitions);
        }
    }

}
