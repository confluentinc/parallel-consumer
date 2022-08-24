package io.confluent.parallelconsumer.internal;

import io.confluent.csid.utils.TimeUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

import java.util.function.Supplier;

/**
 * DI
 * <p>
 * todo docs
 * <p>
 * A-la' Dagger.
 *
 * @author Antony Stubbs
 */
public abstract class PCModule<K, V> {

    @Getter
    protected ParallelConsumerOptions<K, V> optionsInstance;

    @Setter
    protected AbstractParallelEoSStreamProcessor<K, V> parallelEoSStreamProcessor;

    protected PCModule(ParallelConsumerOptions<K, V> options) {
        this.optionsInstance = options;
        options.setModule(this);
    }

    protected ParallelConsumerOptions options() {
        return optionsInstance;
    }

    private ProducerWrap<K, V> kvProducerWrap;

    protected ProducerWrap<K, V> producerWrap() {
        if (this.kvProducerWrap == null) {
            this.kvProducerWrap = new ProducerWrap<>(options());
        }
        return kvProducerWrap;
    }

    private ProducerManager<K, V> kvProducerManager;

    //Provides
    protected ProducerManager<K, V> producerManager() {
        if (kvProducerManager == null) {
            this.kvProducerManager = new ProducerManager<K, V>(producerWrap(), consumerManager(), workManager(), options());
        }
        return kvProducerManager;
    }

    private Producer<K, V> producer() {
        return optionsInstance.getProducer();
    }

    private ConsumerManager consumerManager;

    protected ConsumerManager<K, V> consumerManager() {
        if (consumerManager == null) {
            consumerManager = new ConsumerManager(optionsInstance.getConsumer());
        }
        return consumerManager;
    }

    private WorkManager workManager;

    public WorkManager<K, V> workManager() {
        if (workManager == null) {
            workManager = new WorkManager<K, V>(options(), dynamicExtraLoadFactor(), TimeUtils.getClock());
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

    private BrokerPollSystem brokerPollSystem;

    protected BrokerPollSystem<K, V> brokerPoller(AbstractParallelEoSStreamProcessor<K, V> pc) {
        if (brokerPollSystem == null) {
//            final ParallelEoSStreamProcessor<K, V> pc = pc();
            brokerPollSystem = new BrokerPollSystem<>(consumerManager(), workManager(), pc, options());
        }
        return brokerPollSystem;
    }

    public Supplier<AbstractParallelEoSStreamProcessor<K, V>> pcSupplier() {
        return this::pc;
    }

    public Consumer<K, V> consumer() {
        return optionsInstance.getConsumer();
    }
}