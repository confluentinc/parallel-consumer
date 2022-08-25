package io.confluent.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import io.confluent.parallelconsumer.state.ModelUtils;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.IntStreamEx;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.KafkaContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.google.common.truth.Truth.assertThat;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.ProducerMode.TRANSACTIONAL;
import static java.time.Duration.ofSeconds;
import static java.util.Optional.empty;
import static org.apache.commons.lang3.RandomUtils.nextInt;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
@Slf4j
public class KafkaClientUtils {

    public static final int MAX_POLL_RECORDS = 10_000;
    public static final String GROUP_ID_PREFIX = "group-1-";

    class PCVersion {
        public static final String V051 = "0.5.1";
    }


    private final KafkaContainer kContainer;

    @Getter
    private KafkaConsumer<String, String> consumer;

    @Getter
    private KafkaProducer<String, String> producer;

    @Getter
    private AdminClient admin;
    private String groupId = GROUP_ID_PREFIX + nextInt();

    /**
     * todo docs
     */
    private KafkaConsumer<String, String> lastConsumerConstructed;


    public KafkaClientUtils(KafkaContainer kafkaContainer) {
        kafkaContainer.addEnv("KAFKA_transaction_state_log_replication_factor", "1");
        kafkaContainer.addEnv("KAFKA_transaction_state_log_min_isr", "1");
        kafkaContainer.start();
        this.kContainer = kafkaContainer;
    }

    private Properties setupCommonProps() {
        var commonProps = new Properties();
        String servers = this.kContainer.getBootstrapServers();
        commonProps.put("bootstrap.servers", servers);
        return commonProps;
    }

    private Properties setupProducerProps() {
        var producerProps = setupCommonProps();

        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return producerProps;
    }

    private Properties setupConsumerProps() {
        var consumerProps = setupCommonProps();

        //
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //
        //    consumerProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10);
        //    consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 100);

        // make sure we can download lots of records if they're small. Default is 500
//        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1_000_000);
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);

        return consumerProps;
    }

    @BeforeEach
    public void open() {
        log.info("Setting up clients...");
        consumer = this.createNewConsumer();
        producer = this.createNewProducer(false);
        admin = AdminClient.create(setupCommonProps());
    }

    @AfterEach
    public void close() {
        if (producer != null)
            producer.close();
        if (consumer != null)
            consumer.close();
        if (admin != null)
            admin.close();
    }

    public enum GroupOption {
        REUSE_GROUP,
        NEW_GROUP
    }


    public <K, V> KafkaConsumer<K, V> createNewConsumer(GroupOption reuseGroup) {
        return createNewConsumer(reuseGroup.equals(GroupOption.NEW_GROUP));
    }

    public <K, V> KafkaConsumer<K, V> createNewConsumer() {
        return createNewConsumer(false);
    }

    @Deprecated
    public <K, V> KafkaConsumer<K, V> createNewConsumer(boolean newConsumerGroup) {
        return createNewConsumer(newConsumerGroup, new Properties());
    }

    @Deprecated
    public <K, V> KafkaConsumer<K, V> createNewConsumer(Properties options) {
        return createNewConsumer(false, options);
    }

    @Deprecated
    public <K, V> KafkaConsumer<K, V> createNewConsumer(boolean newConsumerGroup, Properties options) {
        Properties properties = setupConsumerProps();

        if (newConsumerGroup) {
            // overwrite the group id with a new one
            String newGroupId = GROUP_ID_PREFIX + nextInt();
            this.groupId = newGroupId; // save it for reuse later
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, newGroupId); // new group
        }

        // override with custom
        properties.putAll(options);

        KafkaConsumer<K, V> kvKafkaConsumer = new KafkaConsumer<>(properties);
        log.debug("New consume {}", kvKafkaConsumer);
        return kvKafkaConsumer;
    }

    public <K, V> KafkaProducer<K, V> createNewTransactionalProducer() {
        KafkaProducer<K, V> txProd = createNewProducer(TRANSACTIONAL);
        txProd.initTransactions();
        return txProd;
    }

    /**
     * @deprecated use the enum version {@link #createNewProducer(ProducerMode)} instead, since = {@link PCVersion#V051}
     */
    @Deprecated
    public <K, V> KafkaProducer<K, V> createNewProducer(boolean transactional) {
        var mode = transactional ? TRANSACTIONAL : NOT_TRANSACTIONAL;
        return createNewProducer(mode);
    }

    public KafkaProducer<String, String> createNewProducer(CommitMode commitMode) {
        return createNewProducer(ProducerMode.matching(commitMode));
    }

    public <K, V> KafkaProducer<K, V> createNewProducer(ProducerMode mode) {
        Properties properties = setupProducerProps();

        var txProps = new Properties();
        txProps.putAll(properties);

        if (mode.equals(TRANSACTIONAL)) {
            // random number, so we get a unique producer tx session each time. Normally wouldn't do this in production,
            // but sometimes running in the test suite our producers' step on each other between test runs and this causes
            // Producer Fenced exceptions:
            // Error looks like: Producer attempted an operation with an old epoch. Either there is a newer producer with
            // the same transactionalId, or the producer's transaction has been expired by the broker.
            txProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, this.getClass().getSimpleName() + ":" + nextInt()); // required for tx
            txProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, (int) ofSeconds(10).toMillis()); // speed things up
        }

        KafkaProducer<K, V> kvKafkaProducer = new KafkaProducer<>(txProps);

        log.debug("New producer {}", kvKafkaProducer);
        return kvKafkaProducer;
    }

    public enum ProducerMode {
        TRANSACTIONAL, NOT_TRANSACTIONAL;

        public static ProducerMode matching(CommitMode commitMode) {
            return commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER)
                    ? TRANSACTIONAL
                    : NOT_TRANSACTIONAL;
        }
    }

    @SneakyThrows
    public List<NewTopic> createTopics(int numTopics) {
        List<NewTopic> newTopics = IntStreamEx.range(numTopics)
                .mapToObj(i
                        -> new NewTopic("in-" + i + "-" + nextInt(), empty(), empty()))
                .toList();
        getAdmin().createTopics(newTopics)
                .all()
                .get();
        return newTopics;
    }

    public List<String> produceMessages(String topicName, long numberToSend) throws InterruptedException, ExecutionException {
        log.info("Producing {} messages to {}", numberToSend, topicName);
        final List<String> expectedKeys = new ArrayList<>();
        List<Future<RecordMetadata>> sends = new ArrayList<>();
        try (Producer<String, String> kafkaProducer = createNewProducer(false)) {

            var mu = new ModelUtils(new PCModuleTestEnv());
            List<ProducerRecord<String, String>> recs = mu.createProducerRecords(topicName, numberToSend);

            for (var record : recs) {
                Future<RecordMetadata> send = kafkaProducer.send(record, (meta, exception) -> {
                    if (exception != null) {
                        log.error("Error sending, ", exception);
                    }
                });
                sends.add(send);
                expectedKeys.add(record.key());
            }
            log.debug("Finished sending test data");
        }
        // make sure we finish sending before next stage
        log.debug("Waiting for broker acks");
        for (Future<RecordMetadata> send : sends) {
            RecordMetadata recordMetadata = send.get();
            boolean b = recordMetadata.hasOffset();
            assertThat(b).isTrue();
        }
        assertThat(sends).hasSize(Math.toIntExact(numberToSend));
        return expectedKeys;
    }

    public ParallelEoSStreamProcessor<String, String> buildPc(ProcessingOrder order, CommitMode commitMode, int maxPoll) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoll);
        KafkaConsumer<String, String> newConsumer = createNewConsumer(false, consumerProps);
        lastConsumerConstructed = newConsumer;

        var pc = new ParallelEoSStreamProcessor<>(ParallelConsumerOptions.<String, String>builder()
                .ordering(order)
                .consumer(newConsumer)
                .commitMode(commitMode)
                .maxConcurrency(100)
                .build());

        pc.setTimeBetweenCommits(ofSeconds(1));

        // sanity
        return pc;
    }

    public ParallelEoSStreamProcessor<String, String> buildPc(ProcessingOrder key) {
        return buildPc(key, PERIODIC_CONSUMER_ASYNCHRONOUS, 500);
    }

    public KafkaConsumer<String, String> getLastConsumerConstructed() {
        return lastConsumerConstructed;
    }
}
