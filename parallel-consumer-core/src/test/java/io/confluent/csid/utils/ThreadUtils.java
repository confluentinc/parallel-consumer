package io.confluent.csid.utils;

import lombok.SneakyThrows;

public class ThreadUtils {
    @SneakyThrows
    public static void sleepQueietly(final int ms) {
        Thread.sleep(ms);
    }
}
