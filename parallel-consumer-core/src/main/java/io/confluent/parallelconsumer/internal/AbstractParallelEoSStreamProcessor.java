package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContextInternal;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import static lombok.AccessLevel.PRIVATE;
import static lombok.AccessLevel.PROTECTED;

/**
 * @see ParallelConsumer
 */
@Slf4j
@FieldDefaults(level = PRIVATE, makeFinal = true)
public abstract class AbstractParallelEoSStreamProcessor<K, V> implements ParallelConsumer<K, V>, Closeable {

    @Getter(PROTECTED)
    protected ParallelConsumerOptions<K, V> options;

    private StateMachine state;

    @Getter(PROTECTED)
    private Controller<K, V> controller;

    protected AbstractParallelEoSStreamProcessor(ParallelConsumerOptions<K, V> newOptions) {
        this(newOptions, new PCModule<>(newOptions));
    }

    /**
     * Construct the AsyncConsumer by wrapping this passed in conusmer and producer, which can be configured any which
     * way as per normal.
     *
     * @see ParallelConsumerOptions
     */
    protected AbstractParallelEoSStreamProcessor(ParallelConsumerOptions<K, V> newOptions, PCModule<K, V> module) {
        Objects.requireNonNull(newOptions, "Options must be supplied");

        options = newOptions;

        var validator = new ConfigurationValidator<K, V>(options, options.getConsumer());
        validator.validateConfiguration();

        module.setParallelEoSStreamProcessor(this);

        controller = new Controller<>(module);
        state = module.stateMachine();

        log.info("Confluent Parallel Consumer initialise... groupId: {}, Options: {}",
                newOptions.getConsumer().groupMetadata().groupId(),
                newOptions);
    }

    protected void supervisorLoop(Function<PollContextInternal<K, V>, List<Object>> wrappedUserFunc, Consumer<Object> voidCallBack) {
        controller.supervisorLoop(wrappedUserFunc, voidCallBack);
    }

    /**
     * @deprecated moving to Options
     */
    // todo move to options
    @Deprecated
    public void setLongPollTimeout(Duration ofMillis) {
        BrokerPollSystem.setLongPollTimeout(ofMillis);
    }


    @Override
    public void pauseIfRunning() {
        state.pauseIfRunning();
    }

    @Override
    public void resumeIfPaused() {
        state.pauseIfRunning();
    }

}
