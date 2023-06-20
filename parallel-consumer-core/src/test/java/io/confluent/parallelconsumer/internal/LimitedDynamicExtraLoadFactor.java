package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

public class LimitedDynamicExtraLoadFactor extends DynamicLoadFactor {
    @Override
    public int getMaxFactor() {
        return 2;
    }

    @Override
    public int getCurrentFactor() {
        return 2;
    }
}
