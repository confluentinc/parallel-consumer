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

/**
 * Version of the {@link PCModule} in test contexts.
 *
 * @author Antony Stubbs
 */
public class PCModuleTestEnv extends PCModule<String, String> {

    ModelUtils mu = new ModelUtils(this);

    public PCModuleTestEnv(ParallelConsumerOptions<String, String> optionsInstance) {
        super(optionsInstance);

        ParallelConsumerOptions<String, String> override = enhanceOptions(optionsInstance);

        // overwrite super's with new instance
        super.optionsInstance = override;
    }

    private ParallelConsumerOptions<String, String> enhanceOptions(ParallelConsumerOptions<String, String> optionsInstance) {
        var copy = options().toBuilder();

        if (optionsInstance.getConsumer() == null) {
            Consumer<String, String> mockConsumer = Mockito.mock(Consumer.class);
            Mockito.when(mockConsumer.groupMetadata()).thenReturn(mu.consumerGroupMeta());
            copy.consumer(mockConsumer);
        }

        var override = copy
                .producer(Mockito.mock(Producer.class))
                .build();

        return override;
    }

    public PCModuleTestEnv() {
        this(ParallelConsumerOptions.<String, String>builder().build());
    }

    @Override
    protected ProducerWrapper<String, String> producerWrap() {
        return mockProducerWrapTransactional();
    }

    ProducerWrapper<String, String> mockProduceWrap;

    @NonNull
    private ProducerWrapper mockProducerWrapTransactional() {
        if (mockProduceWrap == null) {
            mockProduceWrap = Mockito.spy(new ProducerWrapper<>(options(), true, producer()));
        }
        return mockProduceWrap;
    }

    @Override
    protected ConsumerManager<String, String> consumerManager() {
        ConsumerManager<String, String> consumerManager = super.consumerManager();

        // force update to set cache, otherwise maybe never called (fake consumer)
        consumerManager.updateMetadataCache();

        return consumerManager;
    }
}