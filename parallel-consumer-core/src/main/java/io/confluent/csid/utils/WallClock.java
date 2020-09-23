package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import java.time.Instant;

public class WallClock {

  public Instant getNow(){
    return Instant.now();
  }

}
