package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.TimeUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import io.confluent.parallelconsumer.offsets.OffsetSimultaneousEncoder;
import io.confluent.parallelconsumer.state.*;
import lombok.Setter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

import java.time.Clock;
import java.util.Set;

/**
 * Minimum dependency injection system, modled on how Dagger works.
 * <p>
 * Note: Not using Dagger as PC has a zero dependency policy, and franky it would be overkill for our needs.
 *
 * @author Antony Stubbs
 */
public class PCModule<K, V> {

    protected ParallelConsumerOptions<K, V> optionsInstance;

    /**
     * Settable for reverse injection in bootstrap/inverted situations
     */
    @Setter
    protected AbstractParallelEoSStreamProcessor<K, V> parallelEoSStreamProcessor;

    public PCModule(ParallelConsumerOptions<K, V> options) {
        this.optionsInstance = options;
    }

    public ParallelConsumerOptions<K, V> options() {
        return optionsInstance;
    }

    private ProducerWrap<K, V> producerWrap;

    protected ProducerWrap<K, V> producerWrap() {
        if (this.producerWrap == null) {
            this.producerWrap = new ProducerWrap<>(options());
        }
        return producerWrap;
    }

    private ProducerManager<K, V> producerManager;

    protected ProducerManager<K, V> producerManager() {
        if (producerManager == null) {
            this.producerManager = new ProducerManager<>(producerWrap(), consumerManager(), workManager(), options());
        }
        return producerManager;
    }

    public Producer<K, V> producer() {
        return optionsInstance.getProducer();
    }

    public Consumer<K, V> consumer() {
        return optionsInstance.getConsumer();
    }

    private ConsumerManager<K, V> consumerManager;

    protected ConsumerManager<K, V> consumerManager() {
        if (consumerManager == null) {
            consumerManager = new ConsumerManager<>(optionsInstance.getConsumer());
        }
        return consumerManager;
    }

    private WorkManager<K, V> workManager;

    public WorkManager<K, V> workManager() {
        if (workManager == null) {
            workManager = new WorkManager<>(this, dynamicExtraLoadFactor());
        }
        return workManager;
    }

    protected AbstractParallelEoSStreamProcessor<K, V> pc() {
        if (parallelEoSStreamProcessor == null) {
            parallelEoSStreamProcessor = new ParallelEoSStreamProcessor<>(options(), this);
        }
        return parallelEoSStreamProcessor;
    }

    final DynamicLoadFactor dynamicLoadFactor = new DynamicLoadFactor();

    protected DynamicLoadFactor dynamicExtraLoadFactor() {
        return dynamicLoadFactor;
    }

    private BrokerPollSystem<K, V> brokerPollSystem;

    protected BrokerPollSystem<K, V> brokerPoller(AbstractParallelEoSStreamProcessor<K, V> pc) {
        if (brokerPollSystem == null) {
            brokerPollSystem = new BrokerPollSystem<>(consumerManager(), workManager(), pc, options());
        }
        return brokerPollSystem;
    }

    private PartitionStateManager<K, V> partitionStateManager;

    public PartitionStateManager<K, V> partitionStateManager() {
        if (partitionStateManager == null) {
            partitionStateManager = new PartitionStateManager<>(this, consumer(), shardManager(), options(), clock());
        }
        return partitionStateManager;
    }

    private ShardManager<K, V> shardManager;

    public ShardManager<K, V> shardManager() {
        if (shardManager == null) {
            shardManager = new ShardManager<>(options(), workManager(), clock());
        }
        return shardManager;
    }

    private Clock clock() {
        return TimeUtils.getClock();
    }

    public OffsetMapCodecManager<K, V> createOffsetMapCodecManager() {
        return new OffsetMapCodecManager<>(consumer());
    }

    public int getMaxMetadataSize() {
        return OffsetMapCodecManager.KAFKA_MAX_METADATA_SIZE_DEFAULT;
    }

    private RemovedPartitionState<K, V> removedPartitionStateSingleton;

    public PartitionState<K, V> removedPartitionStateSingleton() {
        if (removedPartitionStateSingleton == null) {
            removedPartitionStateSingleton = new RemovedPartitionState<>(this);
        }
        return removedPartitionStateSingleton;
    }

    public OffsetSimultaneousEncoder createOffsetSimultaneousEncoder(long baseOffsetForPartition, long highestSucceeded, Set<Long> incompleteOffsets) {
        return new OffsetSimultaneousEncoder(baseOffsetForPartition, highestSucceeded, incompleteOffsets);
    }

    /**
     * @see #getPayloadThresholdMultiplier()
     */
    public static final double PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT = 0.75;

    /**
     * Best efforts attempt to prevent usage of offset payload beyond X% - as encoding size test is currently only done
     * per batch, we need to leave some buffer for the required space to overrun before hitting the hard limit where we
     * have to drop the offset payload entirely.
     */
    public double getPayloadThresholdMultiplier() {
        return PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT;
    }

}