package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Callable;

@Slf4j
@UtilityClass
public class TimeUtils {

    public Clock getClock() {
        return Clock.systemUTC();
    }

    @SneakyThrows
    public static <RESULT> RESULT time(final Callable<RESULT> func) {
        long start = System.currentTimeMillis();
        RESULT call = func.call();
        long elapsed = System.currentTimeMillis() - start;
        log.trace("Function took {}", Duration.ofMillis(elapsed));
        return call;

    }
}
