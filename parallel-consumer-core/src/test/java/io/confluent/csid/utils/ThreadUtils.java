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
    public static void sleepQuietly(Duration duration) {
        log.debug("Sleeping for {}", duration);
        Thread.sleep(duration.toMillis());
        log.debug("Woke up (slept for {})", duration);
    }

    public static void sleepLog(long seconds) {
        sleepLog(Duration.ofSeconds(seconds));
    }

    public static void sleepLog(Duration duration) {
        try {
            sleepQuietly(duration);
        } catch (Exception e) {
            log.error(msg("Sleep of {} interrupted", duration), e);
        }
    }

    @SneakyThrows
    public static void sleepQuietly(long ms) {
        sleepQuietly((int) ms);
    }

    public static void sleepSecondsLog(long seconds) {
        sleepLog(Duration.ofSeconds(seconds));
    }

}
