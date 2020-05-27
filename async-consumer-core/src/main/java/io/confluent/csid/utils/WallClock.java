package io.confluent.csid.utils;

import java.time.Instant;

public class WallClock {

  public Instant getNow(){
    return Instant.now();
  }

}
