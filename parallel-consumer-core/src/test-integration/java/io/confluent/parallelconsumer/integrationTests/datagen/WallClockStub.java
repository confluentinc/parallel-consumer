package io.confluent.parallelconsumer.integrationTests.datagen;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.csid.utils.AdvancingWallClockProvider;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.time.Instant;

@RequiredArgsConstructor
public class WallClockStub extends AdvancingWallClockProvider {

    @NonNull
    private final Instant baseTime;

    @Override
    public Instant getNow() {
        return baseTime;
    }

}
