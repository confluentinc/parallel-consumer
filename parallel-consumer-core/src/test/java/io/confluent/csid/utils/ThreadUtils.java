package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */
import lombok.SneakyThrows;

public class ThreadUtils {
    @SneakyThrows
    public static void sleepQuietly(final int ms) {
        Thread.sleep(ms);
    }

    @SneakyThrows
    public static void sleepQuietly(long ms) {
        sleepQuietly((int) ms);
    }
}
