package io.confluent.csid.utils;

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
