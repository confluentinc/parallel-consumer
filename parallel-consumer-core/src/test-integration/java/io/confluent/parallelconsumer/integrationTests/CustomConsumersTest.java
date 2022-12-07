package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

/**
 * @author Antony Stubbs
 */
class CustomConsumersTest extends BrokerIntegrationTest {

    /**
     * Tests that extended consumer can be used with a custom consumer with PC.
     * <p>
     * Test for issue #195 - https://github.com/confluentinc/parallel-consumer/issues/195
     *
     * @see io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor#checkAutoCommitIsDisabled
     */
    @Test
    void extendedConsumer() { // NOSONAR
        Properties properties = getKcu().setupConsumerProps(this.getClass().getSimpleName());
        CustomConsumer<String, String> client = new CustomConsumer<>(properties);

        ParallelConsumerOptions<String, String> options = ParallelConsumerOptions.<String, String>builder()
                .consumer(client)
                .build();

        ParallelEoSStreamProcessor<String, String> pc = new ParallelEoSStreamProcessor<>(options);
    }

    static class CustomConsumer<K, V> extends KafkaConsumer<K, V> {

        String customField = "custom";

        public CustomConsumer(Properties configs) {
            super(configs);
        }

    }
}