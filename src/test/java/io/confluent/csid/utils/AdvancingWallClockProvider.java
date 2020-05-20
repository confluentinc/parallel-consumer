package io.confluent.csid.utils;

import java.time.Duration;
import java.time.Instant;

public class AdvancingWallClockProvider extends WallClock {

    Instant advanceAndGet(Duration time) {
        return getNow().plus(time);
    }

}
