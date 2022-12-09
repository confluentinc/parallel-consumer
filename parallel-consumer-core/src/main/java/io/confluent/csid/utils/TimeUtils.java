package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
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
        return timeWithMeta(func).getResult();
    }

    @SneakyThrows
    public static <RESULT> TimeResult<RESULT> timeWithMeta(final Callable<? extends RESULT> func) {
        long start = System.currentTimeMillis();
        TimeResult.TimeResultBuilder<RESULT> timer = TimeResult.<RESULT>builder().startMs(start);
        RESULT call = func.call();
        timer.result(call);
        long end = System.currentTimeMillis();
        long elapsed = end - start;
        timer.endMs(end);
        log.trace("Function took {}", Duration.ofMillis(elapsed));
        return timer.build();
    }

    @Builder
    @Value
    public static class TimeResult<RESULT> {
        long startMs;
        long endMs;
        RESULT result;

        public Duration getElapsed() {
            return Duration.ofMillis(endMs - startMs);
        }
    }
}
