
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests.utils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.KafkaContainer;

import java.util.Properties;

import static java.time.Duration.ofSeconds;

@Slf4j
public class KafkaClientUtils {

    public static final int MAX_POLL_RECORDS = 10_000;

    private final KafkaContainer kContainer;

    @Getter
    private KafkaConsumer<String, String> consumer;

    @Getter
    private KafkaProducer<String, String> producer;

    @Getter
    private AdminClient admin;

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
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1-" + RandomUtils.nextInt());
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

    public <K, V> KafkaConsumer<K, V> createNewConsumer() {
        return createNewConsumer(false);
    }

    public <K, V> KafkaConsumer<K, V> createNewConsumer(boolean newConsumerGroup) {
        return createNewConsumer(newConsumerGroup, new Properties());
    }

    public <K, V> KafkaConsumer<K, V> createNewConsumer(Properties options) {
        return createNewConsumer(false, options);
    }

    public <K, V> KafkaConsumer<K, V> createNewConsumer(boolean newConsumerGroup, Properties options) {
        Properties properties = setupConsumerProps();

        if (newConsumerGroup) {
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1-" + RandomUtils.nextInt()); // new group
        }

        // override with custom
        properties.putAll(options);

        KafkaConsumer<K, V> kvKafkaConsumer = new KafkaConsumer<>(properties);
        log.debug("New consume {}", kvKafkaConsumer);
        return kvKafkaConsumer;
    }

    public <K, V> KafkaProducer<K, V> createNewProducer(boolean tx) {
        Properties properties = setupProducerProps();

        var txProps = new Properties();
        txProps.putAll(properties);

        if (tx) {
            // random number so we get a unique producer tx session each time. Normally wouldn't do this in production,
            // but sometimes running in the test suite our producers step on each other between test runs and this causes
            // Producer Fenced exceptions:
            // Error looks like: Producer attempted an operation with an old epoch. Either there is a newer producer with
            // the same transactionalId, or the producer's transaction has been expired by the broker.
            txProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, this.getClass().getSimpleName() + ":" + RandomUtils.nextInt()); // required for tx
            txProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, (int) ofSeconds(10).toMillis()); // speed things up

        }
        KafkaProducer<K, V> kvKafkaProducer = new KafkaProducer<>(txProps);

        log.debug("New producer {}", kvKafkaProducer);
        return kvKafkaProducer;
    }
}
