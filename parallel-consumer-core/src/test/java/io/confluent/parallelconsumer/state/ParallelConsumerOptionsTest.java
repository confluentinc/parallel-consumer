package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertThrows;


@Slf4j
public class ParallelConsumerOptionsTest {

    private final Consumer<Object,Object> mockConsumer = Mockito.mock(Consumer.class);


    @Test
    void validateMinBatchParameters(){
        assertThrows(
                IllegalArgumentException.class,
                () -> ParallelConsumerOptions.builder()
                        .minBatchSize(10)
                        .minBatchTimeoutInMillis(100)
                        .consumer(mockConsumer)
                        .batchSize(5)
                        .build()
                        .validate()
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> ParallelConsumerOptions.builder()
                        .minBatchSize(3)
                        .minBatchTimeoutInMillis(-1)
                        .consumer(mockConsumer)
                        .batchSize(5)
                        .build()
                        .validate()
        );
    }
}
