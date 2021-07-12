package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

@Slf4j
public class GeneralTestUtils {

    public static void changeLogLevelTo(Level targetLevel) {
        log.warn("Making sure log level isn't too low");
//        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
//        root.setLevel(Level.INFO);

        Logger csid = (Logger) LoggerFactory.getLogger("io.confluent.csid");
        csid.setLevel(targetLevel);
    }

    @SneakyThrows
    public static Duration time(Runnable c) {
        Instant start = Instant.now();
//        log.debug("Starting at: {}", start);
        c.run();
        Instant end = Instant.now();
        Duration between = Duration.between(start, end);
        log.debug("Finished, took {}", between);
        return between;
    }
}
