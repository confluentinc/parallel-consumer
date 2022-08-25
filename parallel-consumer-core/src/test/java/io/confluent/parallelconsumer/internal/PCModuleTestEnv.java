package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.state.ModelUtils;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public class PCModuleTestEnv extends PCModule<String, String> {

    ModelUtils mu = new ModelUtils(this);

    public PCModuleTestEnv(ParallelConsumerOptions<String, String> optionsInstance) {
        super(optionsInstance);
        Consumer mockConsumer = mock(Consumer.class);
        when(mockConsumer.groupMetadata()).thenReturn(mu.consumerGroupMeta());
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
    protected ProducerWrap<String, String> producerWrap() {
        return mockProducerWrapTransactional();
    }

    ProducerWrap mock = mock(ProducerWrap.class);

    {
        Mockito.when(mock.isConfiguredForTransactions()).thenReturn(true);

    }

    @NonNull
    private ProducerWrap mockProducerWrapTransactional() {
        return mock;
    }

    @Override
    protected ConsumerManager<String, String> consumerManager() {
        ConsumerManager<String, String> consumerManager = super.consumerManager();

        // force update to set cache, otherwise maybe never called (fake consuemr)
        consumerManager.updateMetadataCache();

        return consumerManager;
    }
}