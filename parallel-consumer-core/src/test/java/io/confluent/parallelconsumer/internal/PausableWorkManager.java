package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PausableWorkManager<K, V> extends WorkManager<K, V> {

    private final Optional<CountDownLatch> optionalCountDownLatch;

    public PausableWorkManager(final PCModule<K, V> module, final DynamicLoadFactor dynamicExtraLoadFactor,
                               final CountDownLatch latchToControlWorkIfAvailable) {
        super(module, dynamicExtraLoadFactor);
        optionalCountDownLatch = Optional.of(latchToControlWorkIfAvailable);
    }

    @Override
    public List<WorkContainer<K, V>> getWorkIfAvailable(final int requestedMaxWorkToRetrieve) {
        final List<WorkContainer<K, V>> workContainers = super.getWorkIfAvailable(requestedMaxWorkToRetrieve);
        if (workContainers.size() > 0) {
            this.optionalCountDownLatch.ifPresent(this::awaitLatch);
        }
        return workContainers;
    }

    @SneakyThrows
    void awaitLatch(final CountDownLatch countDownLatch) {
        try {
            countDownLatch.await(60, TimeUnit.SECONDS);
        } catch (final InterruptedException ex) {
            log.debug("Expected the exception on rebalance, continue..",ex);
        }
    }
}
