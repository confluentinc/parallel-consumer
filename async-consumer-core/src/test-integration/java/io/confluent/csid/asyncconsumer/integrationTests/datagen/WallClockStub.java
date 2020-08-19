
/*-
 * Copyright (C) 2020 Confluent, Inc.
 */
package io.confluent.csid.asyncconsumer.integrationTests.datagen;

import io.confluent.csid.utils.AdvancingWallClockProvider;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.time.Instant;

@RequiredArgsConstructor
public class WallClockStub extends AdvancingWallClockProvider {

  @NonNull
  private Instant baseTime;

  @Override
  public Instant getNow() {
    return baseTime;
  }

}
