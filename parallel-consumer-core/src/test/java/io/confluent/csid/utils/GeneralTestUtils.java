package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;

@Slf4j
public class GeneralTestUtils {

    @SneakyThrows
    public static Duration time(Runnable task) {
        Instant start = Instant.now();
        log.debug("Timed function starting at: {}", start);
        task.run();
        Instant end = Instant.now();
        Duration between = Duration.between(start, end);
        log.debug("Finished, took {}", between);
        return between;
    }
}
