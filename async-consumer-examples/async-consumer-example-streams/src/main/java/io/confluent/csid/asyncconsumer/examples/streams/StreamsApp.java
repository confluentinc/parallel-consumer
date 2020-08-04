package io.confluent.csid.asyncconsumer.examples.streams;

import io.confluent.csid.asyncconsumer.AsyncConsumer;
import io.confluent.csid.asyncconsumer.AsyncConsumerOptions;
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

import java.util.List;
import java.util.Properties;
import java.util.Random;
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

    AsyncConsumer<String, String> async;

    AtomicInteger messageCount = new AtomicInteger();

    void run() {
        preprocess();
        concurrentProcess();
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
        var options = AsyncConsumerOptions.builder()
                .ordering(AsyncConsumerOptions.ProcessingOrder.KEY)
                .maxConcurrency(1000)
                .maxUncommittedMessagesToHandle(10000)
                .build();

        Consumer<String, String> kafkaConsumer = getKafkaConsumer();
        kafkaConsumer.subscribe(List.of(outputTopicName));

        async = new AsyncConsumer<>(kafkaConsumer, getKafkaProducer(), options);

        async.asyncPoll(record -> {
            log.info("Concurrently processing a record: {}", record);
            messageCount.getAndIncrement();
        });
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
        async.close();
    }

}
