package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

import static io.confluent.csid.utils.StringUtils.msg;

@Slf4j
public class ThreadUtils {

    @SneakyThrows
    public static void sleepQuietly(final int ms) {
        log.debug("Sleeping for {}", ms);
        Thread.sleep(ms);
        log.debug("Woke up (slept for {})", ms);
    }

    public static void sleepLog(Duration duration) {
        sleepLog(duration.toMillis());
    }

    public static void sleepLog(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            log.error(msg("Sleep of {} interrupted", ms), e);
        }
    }

    @SneakyThrows
    public static void sleepQuietly(long ms) {
        sleepQuietly((int) ms);
    }

    public static void sleepSecondsLog(int seconds) {
        sleepLog(seconds * 1000L);
    }
}
