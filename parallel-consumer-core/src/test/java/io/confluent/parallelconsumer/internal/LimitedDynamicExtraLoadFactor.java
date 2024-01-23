package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

public class LimitedDynamicExtraLoadFactor extends DynamicLoadFactor {
    public LimitedDynamicExtraLoadFactor() {
        super(2, 2);
    }
}
