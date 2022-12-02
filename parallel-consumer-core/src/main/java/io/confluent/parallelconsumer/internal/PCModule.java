package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.TimeUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.Setter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

import java.time.Clock;
import java.util.Optional;

/**
 * Minimum dependency injection system, modled on how Dagger works.
 * <p>
 * Note: Not using Dagger as PC has a zero dependency policy, and franky it would be overkill for our needs.
 *
 * @author Antony Stubbs
 */
//todo make all protected
public class PCModule<K, V> {

    protected ParallelConsumerOptions<K, V> optionsInstance;

    @Setter
    protected AbstractParallelEoSStreamProcessor<K, V> parallelEoSStreamProcessor;

    private WorkMailbox<K, V> workMailbox;

    private StateMachine stateMachine;

    private Controller<K, V> controller;

//    private PCWorkerPool<K, V, Object> workerThreadPool;

    private RebalanceHandler<K, V> rebalanceHandler;

    private ControlLoop<K, V> controlLoop;

    public PCModule(ParallelConsumerOptions<K, V> options) {
        this.optionsInstance = options;
    }

    public ParallelConsumerOptions<K, V> options() {
        return optionsInstance;
    }

    private ProducerWrapper<K, V> producerWrapper;

    protected ProducerWrapper<K, V> producerWrap() {
        if (this.producerWrapper == null) {
            this.producerWrapper = new ProducerWrapper<>(options());
        }
        return producerWrapper;
    }

    private ProducerManager<K, V> producerManager;

    public OffsetCommitter committer() {
        if (options().isUsingTransactionCommitMode())
            //noinspection OptionalGetWithoutIsPresent
            return producerManager().get();
        else
            return brokerPoller();
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

    @Setter
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

    protected BrokerPollSystem<K, V> brokerPoller() {
        if (brokerPollSystem == null) {
            brokerPollSystem = new BrokerPollSystem<>(consumerManager(), workMailbox(), workManager(), options());
        }
        return brokerPollSystem;
    }

    public WorkMailbox<K, V> workMailbox() {
        if (workMailbox == null) {
            workMailbox = new WorkMailbox<>(workManager());
        }
        return workMailbox;
    }

    public Clock clock() {
        return TimeUtils.getClock();
    }

    public RebalanceHandler<K, V> rebalanceHandler() {
        if (rebalanceHandler == null) {
            rebalanceHandler = new RebalanceHandler<>(stateMachine(), controlLoop(), consumer(), controller(), workManager());
        }
        return rebalanceHandler;
    }

    public StateMachine stateMachine() {
        if (stateMachine == null) {
            stateMachine = new StateMachine(this);
        }
        return stateMachine;
    }

    // todo make protected
    public Optional<ProducerManager<K, V>> producerManager() {
        if (!options().isProducerSupplied()) {
            return Optional.empty();
        }

        if (producerManager == null) {
            this.producerManager = new ProducerManager<>(producerWrap(), consumerManager(), workManager(), options());
        }
        return Optional.of(producerManager);
    }

    public SubscriptionHandler subscriptionHandler() {
        return rebalanceHandler();
    }

//    public PCWorkerPool<?, ?, ?> workerThreadPool() {
//        if (workerThreadPool == null) {
//            workerThreadPool = new PCWorkerPool<>(options().getMaxConcurrency(), );
//        }
//        return workerThreadPool;
//    }

    public ControlLoop<K, V> controlLoop() {
        if (controlLoop == null) {
            controlLoop = new ControlLoop<>(this);
        }
        return controlLoop;
    }

    public Controller<K, V> controller() {
        if (controller == null) {
            controller = new Controller<>(this);
        }
        return controller;
    }

    public WorkMailbox<K, V> workMailBox() {
        if (workMailbox == null) {
            workMailbox = new WorkMailbox<>(workManager());
        }
        return workMailbox;
    }
}