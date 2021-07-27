package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ThreadUtils {

    @SneakyThrows
    public static void sleepQuietly(final int ms) {
        Thread.sleep(ms);
    }

    public static void sleepLog(final int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            log.error("Sleep of {} interrupted", e, ms);
        }
    }

    @SneakyThrows
    public static void sleepQuietly(long ms) {
        sleepQuietly((int) ms);
    }
}
