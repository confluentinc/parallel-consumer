
/*-
 * Copyright (C) 2020 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests;

import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.OffsetMapCodecManager;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.csid.utils.Range;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.shaded.org.apache.commons.lang.math.RandomUtils;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Series of tests that check when we close a PC with incompletes encoded, when we open a new one, the correct messages
 * are skipped.
 *
 * @see OffsetMapCodecManager
 */
@Timeout(value = 60)
@Slf4j
public class CloseAndOpenOffsetTest extends BrokerIntegrationTest<String, String> {

    Duration normalTimeout = ofSeconds(5);
    Duration debugTimeout = Duration.ofMinutes(1);

    // use debug timeout while debugging
//    Duration timeoutToUse = debugTimeout;
    Duration timeoutToUse = normalTimeout;

    String rebalanceTopic;

    @BeforeEach
    void setup() {
        rebalanceTopic = "close-and-open-" + RandomUtils.nextInt();
    }

    /**
     * publish some messages some fail shutdown startup again consume again - check we only consume the failed messages
     * <p>
     * Sometimes fails as 5 is not comitted in the first run and comes out in the 2nd
     * <p>
     * NB: messages 4 and 2 are made to fail
     */
    @Timeout(value = 60)
    @SneakyThrows
    @Test
    void offsetsOpenClose() {
        // 2 partition topic
        try {
            ensureTopic(rebalanceTopic, 1);
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }

        // 1 client
        KafkaConsumer<String, String> newConsumerOne = kcu.createNewConsumer();
        KafkaProducer<String, String> producerOne = kcu.createNewProducer(true);
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(UNORDERED)
                .commitMode(TRANSACTIONAL_PRODUCER)
                .consumer(newConsumerOne)
                .producer(producerOne)
                .build();
        kcu.props.put(ConsumerConfig.CLIENT_ID_CONFIG, "ONE-my-client");

        //
        var asyncOne = new ParallelEoSStreamProcessor<String, String>(options);

        //
        asyncOne.subscribe(UniLists.of(rebalanceTopic));

        // read some messages
        var readByOne = new ConcurrentLinkedQueue<ConsumerRecord<String, String>>();
        asyncOne.poll(x -> {
            log.info("Read by consumer ONE: {}", x);
            if (x.value().equals("4")) {
                log.info("Throwing fake error for message 4");
                throw new RuntimeException("Message 4");
            }
            if (x.value().equals("2")) {
                log.info("Throwing fake error for message 2");
                throw new RuntimeException("Message 2");
            }
            readByOne.add(x);
        });

        // wait for initial 0 commit
        Thread.sleep(500);

        //
        send(rebalanceTopic, 0, 0);
        send(rebalanceTopic, 0, 1);
        send(rebalanceTopic, 0, 2);
        send(rebalanceTopic, 0, 3);
        send(rebalanceTopic, 0, 4);
        send(rebalanceTopic, 0, 5);

        // all are processed except msg 2 and 4, which holds up the queue
        await().alias("check all except 2 and 4 are processed").atMost(normalTimeout).untilAsserted(() -> {
                    ArrayList<ConsumerRecord<String, String>> copy = new ArrayList<>(readByOne);
                    assertThat(copy.stream()
                            .map(x -> x.value()).collect(Collectors.toList()))
                            .containsOnly("0", "1", "3", "5");
                }
        );

        // wait until all expected records have been processed and committed
        // need to wait for final message processing's offset data to be committed
        // TODO test for event/trigger instead - could consume offsets topic but have to decode the binary
        // could listen to a produce topic, but currently it doesn't use the produce flow
        // could add a commit listener to the api, but that's heavy just for this?
        // could use Consumer#committed to check and decode, but it's not thread safe
        // sleep is lazy but much much simpler
        Thread.sleep(500);

        // commit what we've done so far, don't wait for failing messages to be retried (message 4)
        log.info("Closing consumer, committing offset map");
        asyncOne.closeDontDrainFirst();

        await().alias("check all except 2 and 4 are processed").atMost(normalTimeout).untilAsserted(() ->
                assertThat(readByOne.stream()
                        .map(x -> x.value()).collect(Collectors.toList()))
                        .containsOnly("0", "1", "3", "5"));


        //
        kcu.props.put(ConsumerConfig.CLIENT_ID_CONFIG, "THREE-my-client");
        KafkaConsumer<String, String> newConsumerThree = kcu.createNewConsumer();
        KafkaProducer<String, String> producerThree = kcu.createNewProducer(true);
        var optionsThree = options.toBuilder().consumer(newConsumerThree).producer(producerThree).build();
        try (var asyncThree = new ParallelEoSStreamProcessor<String, String>(optionsThree)) {
            asyncThree.subscribe(UniLists.of(rebalanceTopic));

            // read what we're given
            var readByThree = new ConcurrentLinkedQueue<ConsumerRecord<String, String>>();
            asyncThree.poll(x -> {
                log.info("Read by consumer THREE: {}", x.value());
                readByThree.add(x);
            });

            // only 2 and 4 should be delivered again, as everything else was processed successfully
            await().atMost(timeoutToUse).untilAsserted(() ->
                    assertThat(readByThree).extracting(ConsumerRecord::value)
                            .containsExactlyInAnyOrder("2", "4"));
        }
    }

    private void send(String topic, int partition, Integer value) throws InterruptedException, ExecutionException {
        RecordMetadata recordMetadata = kcu.producer.send(new ProducerRecord<>(topic, partition, value.toString(), value.toString())).get();
    }

    private void send(int quantity, String topic, int partition) throws InterruptedException, ExecutionException {
        log.debug("Sending {} messages to {}", quantity, topic);
        var futures = new ArrayList<Future<RecordMetadata>>();
        // async
        for (Integer index : Range.range(quantity)) {
            Future<RecordMetadata> send = kcu.producer.send(new ProducerRecord<>(topic, partition, index.toString(), index.toString()));
            futures.add(send);
        }
        // block until finished
        for (Future<RecordMetadata> future : futures) {
            future.get();
        }
        log.debug("Finished sending {} messages", quantity);
    }


    /**
     * Make sure we commit a basic offset correctly - send a single message, read, commit, close, open, read - should be
     * nothing
     */
    @Test
    void correctOffsetVerySimple() {
        setupTopic();

        // send a single message
        kcu.producer.send(new ProducerRecord<>(topic, "0", "0"));

        KafkaConsumer<String, String> consumer = kcu.createNewConsumer();
        KafkaProducer<String, String> producerOne = kcu.createNewProducer(true);
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(UNORDERED)
                .consumer(consumer)
                .producer(producerOne)
                .commitMode(TRANSACTIONAL_PRODUCER)
                .build();

        try (var asyncOne = new ParallelEoSStreamProcessor<String, String>(options)) {

            asyncOne.subscribe(UniLists.of(topic));

            var readByOne = new ArrayList<ConsumerRecord<String, String>>();
            asyncOne.poll(msg -> {
                log.debug("Reading {}", msg);
                readByOne.add(msg);
            });

            // the single message is processed
            await().untilAsserted(() -> assertThat(readByOne)
                    .extracting(ConsumerRecord::value)
                    .containsExactly("0"));

        } finally {
            log.debug("asyncOne closed");
        }

        //
        log.debug("Starting up new client");
        kcu.props.put(ConsumerConfig.CLIENT_ID_CONFIG, "THREE-my-client");
        KafkaConsumer<String, String> newConsumerThree = kcu.createNewConsumer();
        KafkaProducer<String, String> producerThree = kcu.createNewProducer(true);
        ParallelConsumerOptions<String, String> optionsThree = options.toBuilder()
                .consumer(newConsumerThree)
                .producer(producerThree)
                .build();
        try (var asyncThree = new ParallelEoSStreamProcessor<String, String>(optionsThree)) {
            asyncThree.subscribe(UniLists.of(topic));

            // read what we're given
            var readByThree = new ArrayList<ConsumerRecord<String, String>>();
            asyncThree.poll(x -> {
                log.info("Three read: {}", x.value());
                readByThree.add(x);
            });

            // for at least normalTimeout, nothing should be read back (will be long enough to be sure it never is)
            await().alias("nothing should be read back (will be long enough to be sure it never is)")
                    .pollDelay(ofSeconds(1))
                    .atMost(ofSeconds(2))
                    .atLeast(ofSeconds(1))
                    .untilAsserted(() -> {
                                assertThat(readByThree).as("Nothing should be read into the collection")
                                        .extracting(ConsumerRecord::value)
                                        .isEmpty();
                            }
                    );
        }
    }

    /**
     * @see KafkaClientUtils#MAX_POLL_RECORDS
     */
    @SneakyThrows
    @Test
    void largeNumberOfMessagesSmallOffsetBitmap() {
        setupTopic();

        int quantity = 10000;
        assertThat(quantity).as("Test expects to process all the produced messages in a single poll")
                .isLessThanOrEqualTo(KafkaClientUtils.MAX_POLL_RECORDS);
        send(quantity, topic, 0);

        var baseOptions = ParallelConsumerOptions.<String, String>builder()
                .ordering(UNORDERED)
                .commitMode(TRANSACTIONAL_PRODUCER)
                .build();

        Set<String> failingMessages = UniSets.of("123", "2345", "8765");
        int numberOfFailingMessages = failingMessages.size();

        // step 1
        {
            KafkaConsumer<String, String> consumer = kcu.createNewConsumer();
            KafkaProducer<String, String> producerOne = kcu.createNewProducer(true);
            var options = baseOptions.toBuilder()
                    .consumer(consumer)
                    .producer(producerOne)
                    .build();
            var asyncOne = new ParallelEoSStreamProcessor<String, String>(options);

            asyncOne.subscribe(UniLists.of(topic));

            var readByOne = new ConcurrentSkipListSet<String>();
            asyncOne.poll(x -> {
                String value = x.value();
                if (failingMessages.contains(value)) {
                    throw new RuntimeException("Fake error for message " + value);
                }
                readByOne.add(value);
            });

            // the single message is not processed
            await().atMost(ofSeconds(10)).untilAsserted(() -> assertThat(readByOne.size())
                    .isEqualTo(quantity - numberOfFailingMessages));

            //
            // TODO: fatal vs retriable exceptions. Retry limits particularly for draining state?
            asyncOne.closeDontDrainFirst();

            // sanity - post close
            assertThat(readByOne.size()).isEqualTo(quantity - numberOfFailingMessages);
        }

        // step 2
        {
            //
            kcu.props.put(ConsumerConfig.CLIENT_ID_CONFIG, "THREE-my-client");
            KafkaConsumer<String, String> newConsumerThree = kcu.createNewConsumer();
            KafkaProducer<String, String> producerThree = kcu.createNewProducer(true);
            var optionsThree = baseOptions.toBuilder()
                    .consumer(newConsumerThree)
                    .producer(producerThree)
                    .build();
            try (var asyncThree = new ParallelEoSStreamProcessor<String, String>(optionsThree)) {
                asyncThree.subscribe(UniLists.of(topic));

                // read what we're given
                var readByThree = new ConcurrentSkipListSet<String>();
                asyncThree.poll(x -> {
                    log.info("Three read: {}", x.value());
                    readByThree.add(x.value());
                });

                await().alias("Only the one remaining failing message should be submitted for processing")
                        .pollDelay(ofSeconds(1))
                        .atLeast(ofSeconds(1))
                        .untilAsserted(() -> {
                                    assertThat(readByThree.size()).as("Contains only previously failed messages")
                                            .isEqualTo(numberOfFailingMessages);
                                }
                        );

                //
                assertThat(readByThree).hasSize(numberOfFailingMessages); // double check after closing
            }
        }
    }


}
