
/*-
 * Copyright (C) 2020 Confluent, Inc.
 */
package io.confluent.csid.asyncconsumer.integrationTests.utils;

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

import java.util.Map;
import java.util.Properties;

import static java.time.Duration.ofSeconds;

@Slf4j
public class KafkaClientUtils {

    public static final int MAX_POLL_RECORDS = 10_000;
    private final KafkaContainer kContainer;
    public Properties props = new Properties();

    public KafkaConsumer<String, String> consumer;

    public KafkaProducer<String, String> producer;

    public AdminClient admin;

    public KafkaClientUtils(KafkaContainer kafkaContainer) {
        kafkaContainer.addEnv("KAFKA_transaction_state_log_replication_factor", "1");
        kafkaContainer.addEnv("KAFKA_transaction_state_log_min_isr", "1");
        kafkaContainer.start();
        this.kContainer = kafkaContainer;
        setupProps();
    }

    public void setupProps() {
        String servers = this.kContainer.getBootstrapServers();

        props.put("bootstrap.servers", servers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1-" + RandomUtils.nextInt());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        //
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        //
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, (int) ofSeconds(10).toMillis()); // speed things up

        //
//    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10);
//    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 100);

        // make sure we can download lots of records if they're small. Default is 500
//        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1_000_000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
    }

    @BeforeEach
    public void open() {
        log.info("Setting up clients...");
        consumer = this.createNewConsumer();
        producer = this.createNewProducer(false);
        admin = AdminClient.create(props);
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
        if (newConsumerGroup) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1-" + RandomUtils.nextInt()); // new group
        }
        KafkaConsumer<K, V> kvKafkaConsumer = new KafkaConsumer<>(props);
        log.debug("New consume {}", kvKafkaConsumer);
        return kvKafkaConsumer;
    }

    public <K, V> KafkaProducer<K, V> createNewProducer(boolean tx) {
        var txProps = new Properties();
        txProps.putAll(props);
        if (tx) {
            txProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, this.getClass().getSimpleName()); // required for tx
        }
        KafkaProducer<K, V> kvKafkaProducer = new KafkaProducer<>(txProps);
        log.debug("New producer {}", kvKafkaProducer);
        return kvKafkaProducer;
    }
}
