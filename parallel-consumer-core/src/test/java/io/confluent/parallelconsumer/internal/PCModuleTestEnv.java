package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.offsets.ForcedOffsetSimultaneousEncoder;
import io.confluent.parallelconsumer.offsets.OffsetEncoding;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import io.confluent.parallelconsumer.offsets.OffsetSimultaneousEncoder;
import io.confluent.parallelconsumer.state.ModelUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.mock;

/**
 * Version of the {@link PCModule} in test contexts.
 *
 * @author Antony Stubbs
 */
public class PCModuleTestEnv extends PCModule<String, String> {

    protected ModelUtils mu = new ModelUtils(this);

    public PCModuleTestEnv(ParallelConsumerOptions<String, String> optionsInstance) {
        super(optionsInstance);

        ParallelConsumerOptions<String, String> override = enhanceOptions(optionsInstance);

        // overwrite super's with new instance
        super.optionsInstance = override;
    }

    private ParallelConsumerOptions<String, String> enhanceOptions(ParallelConsumerOptions<String, String> optionsInstance) {
        var copy = options().toBuilder();

        if (optionsInstance.getConsumer() == null) {
            Consumer<String, String> mockConsumer = mock(Consumer.class);
            Mockito.when(mockConsumer.groupMetadata()).thenReturn(mu.consumerGroupMeta());
            copy.consumer(mockConsumer);
        }

        var override = copy
                .producer(mock(Producer.class))
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

    @Setter
    private int maxMetadataSize = 4096;

    @Override
    public int getMaxMetadataSize() {
        return maxMetadataSize;
    }

    /**
     * Forces the use of a specific codec, instead of choosing the most efficient one. Useful for testing.
     */
    @Setter
    @Getter
    private Optional<OffsetEncoding> forcedCodec = Optional.empty();

    /**
     * Force the encoder to also add the compressed versions. Useful for testing.
     */
    public boolean compressionForced = false;

    @Override
    public OffsetSimultaneousEncoder createOffsetSimultaneousEncoder(final long baseOffsetForPartition, final long highestSucceeded, final Set<Long> incompleteOffsets) {
        return new ForcedOffsetSimultaneousEncoder(this, baseOffsetForPartition, highestSucceeded, incompleteOffsets);
    }

    @Override
    public OffsetMapCodecManager<String, String> createOffsetMapCodecManager() {
        return new OffsetMapCodecManager<>(consumer());
    }

    /**
     * @see #getPayloadThresholdMultiplier()
     */
    @Setter
    private double payloadThresholdMultiplier = PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT;

    @Override
    public double getPayloadThresholdMultiplier() {
        return payloadThresholdMultiplier;
    }

}