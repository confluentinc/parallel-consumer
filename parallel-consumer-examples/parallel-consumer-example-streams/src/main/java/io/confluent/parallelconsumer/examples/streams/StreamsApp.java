package io.confluent.parallelconsumer.examples.streams;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */


import io.confluent.parallelconsumer.ParallelConsumerImpl;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import pl.tlinkowski.unij.api.UniLists;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class StreamsApp {

    static String inputTopic = "input-topic-" + RandomUtils.nextInt();

    static String outputTopicName = "my-output-topic-" + RandomUtils.nextInt();

    Consumer<String, String> getKafkaConsumer() {
        return new KafkaConsumer<>(new Properties());
    }

    Producer<String, String> getKafkaProducer() {
        return new KafkaProducer<>(new Properties());
    }

    KafkaStreams streams;

    ParallelConsumerImpl<String, String> parallelConsumer;

    AtomicInteger messageCount = new AtomicInteger();

    // tag::example[]
    void run() {
        preprocess(); // <1>
        concurrentProcess(); // <2>
    }

    void preprocess() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(inputTopic)
                .mapValues((key, value) -> {
                    log.info("Streams preprocessing key: {} value: {}", key, value);
                    return String.valueOf(value.length());
                })
                .to(outputTopicName);

        startStreams(builder.build());
    }

    void startStreams(Topology topology) {
        streams = new KafkaStreams(topology, getStreamsProperties());
        streams.start();
    }

    void concurrentProcess() {
        setupConsumer();

        parallelConsumer.poll(record -> {
            log.info("Concurrently processing a record: {}", record);
            messageCount.getAndIncrement();
        });
    }
    // end::example[]

    private void setupConsumer() {
        var options = ParallelConsumerOptions.builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .maxConcurrency(1000)
                .maxUncommittedMessagesToHandlePerPartition(10000)
                .build();

        Consumer<String, String> kafkaConsumer = getKafkaConsumer();

        parallelConsumer = new ParallelConsumerImpl<>(kafkaConsumer, getKafkaProducer(), options);
        parallelConsumer.subscribe(UniLists.of(outputTopicName));
    }

    Properties getStreamsProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, this.getClass().getName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getServerConfig());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    String getServerConfig() {
        return "add your server here";
    }

    void close() {
        streams.close();
        parallelConsumer.close();
    }

}
