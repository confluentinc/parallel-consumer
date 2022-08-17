package io.confluent.parallelconsumer.examples.streams;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */


import io.confluent.parallelconsumer.*;
import io.confluent.parallelconsumer.internal.PCTopologyBuilderImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;
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

    ParallelStreamProcessor<String, String> parallelConsumer;

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
        setupParallelConsumer();

        parallelConsumer.poll(record -> {
            log.info("Concurrently processing a record: {}", record);
            messageCount.getAndIncrement();
        });

        // for reference - KS
        StreamsBuilder ksBuilder = new StreamsBuilder();
        var stream = ksBuilder.<String, String>stream(inputTopic);
        var table = ksBuilder.<String, String>stream(inputTopic).toTable();

        //
        var builder = new PCTopologyBuilderImpl();


        //
        {
            builder.stream("topic-one")
                    .map(this::applyMap)
                    .to("topic-one-output");

            builder.stream("topic-one")
                    .flatMap(this::applyFlatMap)
                    .to("topic-one-output");
        }


        //
        {
            PCStream<String, String> s3 = builder.stream("topic-two");

            s3.foreach(this::callService);

            s3.map(this::applyMap)
                    .to("out-topic");

            parallelConsumer.start(builder.build());
        }


        // custom serde
        {
            // specific serdes
            Serde<String> stringSerde = Serdes.String();
            Consumed<String, String> consumedAsString = Consumed.with(stringSerde, stringSerde);
            Produced<String, String> producedAsString = Produced.with(stringSerde, stringSerde);

            //
            PCStream<String, String> s3 = builder.stream("topic-two", consumedAsString);
            s3.map(this::applyMap)
                    .to("topic-two-out", producedAsString);

            parallelConsumer.start(builder.build());
        }

        // default serdes
        {
            //
            Serde<String> stringSerde = Serdes.String();
            builder = new PCTopologyBuilderImpl(stringSerde);

            //
            PCStream<String, String> s3 = builder.stream("topic-two");
            s3.map(this::applyMap)
                    .to("topic-two-out");

            parallelConsumer.start(builder.build());
        }

        // default serdes
        {
            //
            Serde<String> stringSerde = Serdes.String();
            Serde<Long> longSerde = Serdes.Long();
            builder = new PCTopologyBuilderImpl(stringSerde, longSerde);

            //
            PCStream<String, String> s3 = builder.stream("topic-two");
            s3.map(this::applyMap)
                    .to("topic-two-out");

            parallelConsumer.start(builder.build());
        }


        // table ideas
        //        builder.stream("topic-three").join(table, (left, right) -> left);
    }

    private List<ProducerRecord<Object, Object>> applyMap(PollContext<Object, Object> recordContexts) {
        return null;
    }

    private Iterable<? extends KeyValue<?, ?>> applyFlatMap(PollContext<Object, Object> context) {
        return null;
    }

    private List<ProducerRecord<String, String>> applyMap(PollContext context) {
        return null;
    }

    private void callService(Object o) {

    }
    // end::example[]

    private void setupParallelConsumer() {
        Consumer<String, String> kafkaConsumer = getKafkaConsumer();
        Producer<String, String> kafkaProducer = getKafkaProducer();
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build();


        parallelConsumer = ParallelStreamProcessor.createEosStreamProcessor(options);
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
