package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.producer.Producer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public class PCModuleTestEnv extends PCModule<String, String> {

    public PCModuleTestEnv(ParallelConsumerOptions<String, String> optionsInstance) {
        super(optionsInstance);
        Consumer mockConsumer = mock(Consumer.class);
        when(mockConsumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata(""));
        var override = options().toBuilder()
                .consumer(mockConsumer)
                .producer(mock(Producer.class))
                .build();
        super.optionsInstance = override;
    }

    public PCModuleTestEnv() {
        this(ParallelConsumerOptions.<String, String>builder().build());
    }

    @Override
    protected ConsumerManager<String, String> consumerManager() {
        ConsumerManager<String, String> consumerManager = super.consumerManager();

        // force update to set cache, otherwise maybe never called (fake consuemr)
        consumerManager.updateMetadataCache();

        return consumerManager;
    }
}