package io.confluent.csid.utils;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.Callable;

@Slf4j
@UtilityClass
public class TimeUtils {

    @SneakyThrows
    public static <RESULT> RESULT time(final Callable<RESULT> func) {
        long start = System.currentTimeMillis();
        RESULT call = func.call();
        long elapsed = System.currentTimeMillis() - start;
        log.trace("Function took {}", Duration.ofMillis(elapsed));
        return call;

    }
}
