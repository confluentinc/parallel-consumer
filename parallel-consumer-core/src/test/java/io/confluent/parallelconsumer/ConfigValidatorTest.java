package io.confluent.parallelconsumer;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import pl.tlinkowski.unij.api.UniLists;

import java.util.Properties;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static pl.tlinkowski.unij.api.UniLists.of;


@Slf4j
class ConfigValidatorTest {

    String inputTopic = "test-topic";

    public ParallelStreamProcessor getParallelConsumer() {
        org.apache.kafka.clients.consumer.Consumer<String, String> kafkaConsumer = getKafkaConsumer(); // (1)

        ParallelConsumerOptions<String, String> options = ParallelConsumerOptions.<String, String>builder()
                .ordering(KEY) // (2)
                .maxConcurrency(1000) // (3)
                .consumer(kafkaConsumer)
                .build();

        ParallelStreamProcessor<String, String> eosStreamProcessor =
                ParallelStreamProcessor.createEosStreamProcessor(options);

        eosStreamProcessor.subscribe(of(inputTopic)); // (4)

        return eosStreamProcessor;
    }

    org.apache.kafka.clients.consumer.Consumer<String, String> getKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(properties);
    }

    @Test
    void consume() {
        getParallelConsumer().poll(record ->
                log.info("Concurrently processing a record: {}", record));
        throw new RuntimeException();
    }

    @Test
    void autoCommitConfigTest() {
        ConfigurationValidator<Object, Object> objectObjectConfigurationValidator = new ConfigurationValidator<>(ParallelConsumerOptions.builder().build());
        objectObjectConfigurationValidator.validate();
        throw new RuntimeException();

    }
}