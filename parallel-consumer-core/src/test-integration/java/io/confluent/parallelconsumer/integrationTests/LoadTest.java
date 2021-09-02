package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import pl.tlinkowski.unij.api.UniLists;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static io.confluent.csid.utils.GeneralTestUtils.time;
import static io.confluent.csid.utils.Range.range;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
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

        KafkaConsumer<String, String> newConsumer = kcu.createNewConsumer();
        //
        boolean tx = true;
        ParallelConsumerOptions<String, String> options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .producer(kcu.createNewProducer(tx))
                .consumer(newConsumer)
                .maxConcurrency(3)
                .build();

        ParallelEoSStreamProcessor<String, String> async = new ParallelEoSStreamProcessor<>(options);
        async.subscribe(Pattern.compile(topic));

        AtomicInteger msgCount = new AtomicInteger(0);

        ProgressBar pb = ProgressBarUtils.getNewMessagesBar(log, total);

        try (pb) {
            async.poll(r -> {
                // message processing function
                sleepABit();
                // db isn interesting but not a great performance test, as the db quickly becomes the bottleneck, need to test against a db cluster that can scale better
                // save to db
//                savePayload(r.key(), r.value());
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

    private void sleepABit() {
        int simulatedCPUMessageProcessingDelay = nextInt(0, 5); // random delay between 0,5
        try {
            Thread.sleep(simulatedCPUMessageProcessingDelay); // simulate json parsing overhead and network calls
        } catch (Exception ignore) {
        }
    }

    private void readRecordsPlainConsumer(int total, String topic) {
        // read
        log.info("Starting to read back");
        final List<ConsumerRecord<String, String>> allRecords = Lists.newArrayList();
        AtomicInteger count = new AtomicInteger();
        time(() -> {
            ProgressBar pb = ProgressBarUtils.getNewMessagesBar(log, total);

            Executors.newCachedThreadPool().submit(() -> {
                while (allRecords.size() < total) {
                    ConsumerRecords<String, String> poll = kcu.consumer.poll(ofMillis(500));
                    log.info("Polled batch of {} messages", poll.count());

                    //save
                    Iterable<ConsumerRecord<String, String>> records = poll.records(topic);
                    records.forEach(x -> {
                        // log.trace(x.toString());
                        sleepABit();
                        // db isn interesting but not a great performance test, as the db quickly becomes the bottleneck, need to test against a db cluster that can scale better
//                        savePayload(x.key(), x.value());
                        pb.step();
                        // log.debug(testDataEbean.toString());
                    });

                    //
                    ArrayList<ConsumerRecord<String, String>> c = Lists.newArrayList(records);
                    allRecords.addAll(c);
                    count.getAndAdd(c.size());
                }
            });

            try (pb) {
                await().atMost(ofSeconds(60)).untilAsserted(() -> {
                    assertThat(count).hasValue(total);
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
