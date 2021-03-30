package io.confluent.parallelconsumer.external;

import io.confluent.parallelconsumer.ConfigurationValidator;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
class ConfigValidatorTest {

    String inputTopic = "test-topic";

    public ParallelStreamProcessor getParallelConsumer() {
        //org.apache.kafka.clients.consumer.Consumer<String, String> kafkaConsumer = getKafkaConsumer(); // (1)

        ParallelConsumerOptions<String, String> options = ParallelConsumerOptions.<String, String>builder()
                .ordering(KEY) // (2)
                .maxConcurrency(1000) // (3)
//                .consumer(kafkaConsumer)
                .consumerConfig(getConsumerConfig())
                .build();

        ParallelStreamProcessor<String, String> eosStreamProcessor =
                ParallelStreamProcessor.createEosStreamProcessor(options);

        eosStreamProcessor.subscribe(of(inputTopic)); // (4)

        return eosStreamProcessor;
    }

    org.apache.kafka.clients.consumer.Consumer<String, String> getKafkaConsumer() {
        Properties properties = getConsumerConfig();
        return new KafkaConsumer<>(properties);
    }

    private Properties getConsumerConfig() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "a-group");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }

    @Test
    void consume() {
        ParallelStreamProcessor parallelConsumer = getParallelConsumer();
        parallelConsumer.poll(record ->
                log.info("Concurrently processing a record: {}", record));
    }

    @Test
    void autoCommitConfigTest() {
        ConfigurationValidator<Object, Object> objectObjectConfigurationValidator = new ConfigurationValidator<>(ParallelConsumerOptions.builder().build());
        objectObjectConfigurationValidator.validate();
//        throw new RuntimeException();
    }

    @Test
    void missingConsumer() {
        ParallelConsumerOptions<Object, Object> build = ParallelConsumerOptions.builder()
                .build();
        ConfigurationValidator<Object, Object> objectObjectConfigurationValidator = new ConfigurationValidator<>(build);
        objectObjectConfigurationValidator.validate();
//        throw new RuntimeException();
    }

    @Test
    void bothConfigAndSupplier() {
        ParallelConsumerOptions<Object, Object> build = ParallelConsumerOptions.builder()
                .consumer(null)
                .consumerSupplier(null)
                .build();
        ConfigurationValidator<Object, Object> objectObjectConfigurationValidator = new ConfigurationValidator<>(build);
        objectObjectConfigurationValidator.validate();
//        throw new RuntimeException();
    }

    @Test
    void bothProducerConfigAndSupplier() {
        ParallelConsumerOptions<Object, Object> build = ParallelConsumerOptions.builder()
                .producer(null)
                .producerSupplier(null)
                .build();
        ConfigurationValidator<Object, Object> objectObjectConfigurationValidator = new ConfigurationValidator<>(build);
        objectObjectConfigurationValidator.validate();
//        throw new RuntimeException();
    }
}