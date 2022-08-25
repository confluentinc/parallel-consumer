package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public class PCModuleProd<K, V> extends PCModule<K, V> {

    public PCModuleProd(final ParallelConsumerOptions<K, V> optionsInstance) {
        super(optionsInstance);
    }
}
