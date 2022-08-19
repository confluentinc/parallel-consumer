package io.confluent.parallelconsumer.internal;

import io.confluent.csid.utils.TimeUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

/**
 * DI
 * <p>
 * todo docs
 * <p>
 * A-la' Dagger.
 *
 * @author Antony Stubbs
 */
// Module
@FieldDefaults(level = AccessLevel.PRIVATE)
public abstract class PCModule<K, V> {

    @Getter
    protected ParallelConsumerOptions<K, V> optionsInstance;

    protected PCModule(ParallelConsumerOptions<K, V> optionsInstance) {
        this.optionsInstance = optionsInstance;
    }

    protected ParallelConsumerOptions options() {
        return optionsInstance;
    }

    ProducerWrap<K, V> kvProducerWrap;

    protected ProducerWrap<K, V> producerWrap() {
        if (this.kvProducerWrap == null) {
            this.kvProducerWrap = new ProducerWrap<>(options());
        }
        return kvProducerWrap;
    }

    ProducerManager<K, V> kvProducerManager;

    //Provides
    protected ProducerManager<K, V> producerManager() {
        if (kvProducerManager == null) {
             this.kvProducerManager = new ProducerManager<K, V>(producerWrap(), consumerManager(), workManager(), options());
        }
        return kvProducerManager;
    }

    ConsumerManager consumerManager;

    protected ConsumerManager<K, V> consumerManager() {
        if (consumerManager == null) {
            consumerManager = new ConsumerManager(optionsInstance.getConsumer());
        }
        return consumerManager;
    }

    WorkManager workManager;

    public WorkManager<K, V> workManager() {
        if (workManager == null) {
            workManager = new WorkManager<K, V>(options(), dynamicExtraLoadFactor(), TimeUtils.getClock());
        }
        return workManager;
    }

    ParallelEoSStreamProcessor parallelEoSStreamProcessor;

    protected ParallelEoSStreamProcessor<K, V> pc() {
        if (parallelEoSStreamProcessor == null) {
            parallelEoSStreamProcessor = new ParallelEoSStreamProcessor<>(options(), this);
        }
        return parallelEoSStreamProcessor;
    }

    final DynamicLoadFactor dynamicLoadFactor = new DynamicLoadFactor();

    protected DynamicLoadFactor dynamicExtraLoadFactor() {
        return dynamicLoadFactor;
    }

    BrokerPollSystem brokerPollSystem;

    protected BrokerPollSystem<K, V> brokerPoller(AbstractParallelEoSStreamProcessor<K, V> pc) {
        if (brokerPollSystem == null) {
//            final ParallelEoSStreamProcessor<K, V> pc = pc();
            brokerPollSystem = new BrokerPollSystem<>(consumerManager(), workManager(), pc, options());
        }
        return brokerPollSystem;
    }
}