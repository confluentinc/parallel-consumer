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
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.IntStreamEx;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.ProducerMode.TRANSACTIONAL;
import static java.time.Duration.ofSeconds;
import static java.util.Optional.empty;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Utilities for creating and manipulating clients
 * <p>
 * Caution: When creating new consumers with new group ids, the old group id is overwritten and so cannot be
 * automatically be reused anymore.
 *
 * @author Antony Stubbs
 */
@Slf4j
public class KafkaClientUtils implements AutoCloseable {

    public static final int MAX_POLL_RECORDS = 10_000;

    public static final String GROUP_ID_PREFIX = "group-1-";

    public static final int GROUP_SESSION_TIMEOUT_MS = 5000;

    private final ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

    public AdminClient getAdmin() {
        return admin;
    }

    class PCVersion {
        public static final String V051 = "0.5.1";
    }

    private final PCTestBroker kafkaContainer;

    @Getter
    private KafkaConsumer<String, String> consumer;

    @Setter
    private OffsetResetStrategy offsetResetPolicy = OffsetResetStrategy.EARLIEST;

    @Getter
    private KafkaProducer<String, String> producer;

    private AdminClient admin;

    @Getter
    @Setter
    private String groupId = GROUP_ID_PREFIX + nextInt();

    /**
     * Track all created clients, so they can all be closed
     */
    private final List<Closeable> clientsCreated = new ArrayList<>();

    /**
     * todo docs
     */
    private KafkaConsumer<String, String> lastConsumerConstructed;

    public KafkaClientUtils(PCTestBroker kafkaContainer) {
        this.kafkaContainer = kafkaContainer;
    }

    private Properties createCommonProperties() {
        var commonProps = new Properties();
        String servers = kafkaContainer.getDirectBootstrapServers();
        commonProps.put(BOOTSTRAP_SERVERS_CONFIG, servers);
        commonProps.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
        commonProps.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        commonProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, getClass().getSimpleName() + "-client-" + System.currentTimeMillis());
        return commonProps;
    }

    private Properties setupProducerProps() {
        var producerProps = createCommonProperties();

        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return producerProps;
    }

    public Properties setupConsumerProps(String groupIdToUse) {
        var consumerProps = createCommonProperties();

        //
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupIdToUse);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));

        // Reset
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetPolicy.name().toLowerCase());

        //
        //    consumerProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10);
        //    consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 100);

        // make sure we can download lots of records if they're small. Default is 500 (too few)
//        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1_000_000);
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);


        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, GROUP_SESSION_TIMEOUT_MS);

        return consumerProps;
    }

    public void open() {
        var commonProperties = createCommonProperties();
        log.info("Setting up clients using {}...", commonProperties);
        consumer = this.createNewConsumer();
        producer = this.createNewProducer(false);
        admin = AdminClient.create(commonProperties);
    }

    public void close() {
        if (producer != null)
            producer.close();
        if (consumer != null)
            consumer.close();
        if (admin != null)
            admin.close();

        for (Closeable client : clientsCreated) {
            try {
                client.close();
            } catch (Exception e) {
                log.error("Error closing client", e);
            }
        }
    }

    public enum GroupOption {
        REUSE_GROUP,
        NEW_GROUP
    }


    public <K, V> KafkaConsumer<K, V> createNewConsumer(String groupId) {
        return createNewConsumer(groupId, new Properties());
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

    public <K, V> KafkaConsumer<K, V> createNewConsumer(boolean newConsumerGroup, Properties options) {
        if (newConsumerGroup) {
            // overwrite the group id with a new one
            String newGroupId = GROUP_ID_PREFIX + nextInt();
            this.groupId = newGroupId; // save it for reuse later
        }
        return createNewConsumer(this.groupId, options);
    }

    public <K, V> KafkaConsumer<K, V> createNewConsumer(String groupId, Properties overridingOptions) {
        Properties properties = setupConsumerProps(groupId);

        // override with custom
        properties.putAll(overridingOptions);

        KafkaConsumer<K, V> kvKafkaConsumer = new KafkaConsumer<>(properties);
        clientsCreated.add(kvKafkaConsumer);
        log.debug("New consumer {}", kvKafkaConsumer);
        return kvKafkaConsumer;
    }

    /**
     * Initialises the producer as well, so can't use with PC
     */
    public <K, V> KafkaProducer<K, V> createAndInitNewTransactionalProducer() {
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


    public <K, V> KafkaProducer<K, V> createNewProducer(ProducerMode transactional) {
        return createNewProducer(transactional, new Properties());
    }

    public <K, V> KafkaProducer<K, V> createNewProducer(ProducerMode mode, Properties overridingOptions) {
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

        txProps.putAll(overridingOptions);

        // needed by what test?
//         todo remove?
//        txProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);

        KafkaProducer<K, V> kvKafkaProducer = new KafkaProducer<>(txProps);
        clientsCreated.add(kvKafkaProducer);
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
        return produceMessages(topicName, numberToSend, "");
    }

    public List<String> produceMessages(String topicName, long numberToSend, String prefix) throws InterruptedException, ExecutionException {
        log.info("Producing {} messages to {}", numberToSend, topicName);
        final List<String> expectedKeys = new ArrayList<>();
        List<Future<RecordMetadata>> sends = new ArrayList<>();
        try (Producer<String, String> kafkaProducer = createNewProducer(false)) {

            List<ProducerRecord<String, String>> recs = mu.createProducerRecords(topicName, numberToSend, prefix);

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


    public ParallelEoSStreamProcessor<String, String> buildPc(ProcessingOrder order, CommitMode commitMode, int maxPoll, GroupOption groupOption) {
        return buildPc(ParallelConsumerOptions.<String, String>builder()
                .ordering(order)
                .commitMode(commitMode)
                // todo resolve - magic number
                // 100 27s, 200 23s
                .maxConcurrency(100)
                .build(), groupOption, maxPoll);
    }

    public ParallelEoSStreamProcessor<String, String> buildPc(ProcessingOrder order, CommitMode commitMode, int maxPoll) {
        return buildPc(order, commitMode, maxPoll, GroupOption.REUSE_GROUP);
    }

    public ParallelEoSStreamProcessor<String, String> buildPc(ParallelConsumerOptions<String, String> options, GroupOption groupOption, int maxPoll) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoll);

        // todo test is this needed for? make specific to that test
        //consumerProps.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1);

        if (options.getConsumer() == null) {
            boolean newConsumerGroup = groupOption.equals(GroupOption.NEW_GROUP);
            KafkaConsumer<String, String> newConsumer = createNewConsumer(newConsumerGroup, consumerProps);
            lastConsumerConstructed = newConsumer;
            options = options.toBuilder().consumer(newConsumer).build();
        }

        var pc = new ParallelEoSStreamProcessor<>(options);

        pc.setTimeBetweenCommits(ofSeconds(1));

        // sanity
        return pc;
    }

    public ParallelEoSStreamProcessor<String, String> buildPc(ProcessingOrder key, GroupOption groupOption) {
        return buildPc(key, PERIODIC_CONSUMER_ASYNCHRONOUS, 500, groupOption);
    }

    public ParallelEoSStreamProcessor<String, String> buildPc(ProcessingOrder key) {
        return buildPc(key, PERIODIC_CONSUMER_ASYNCHRONOUS, 500);
    }

    public ParallelEoSStreamProcessor<String, String> buildPc() {
        return buildPc(KEY);
    }

    public KafkaConsumer<String, String> getLastConsumerConstructed() {
        return lastConsumerConstructed;
    }

}
