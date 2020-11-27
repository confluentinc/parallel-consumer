package io.confluent.csid.utils;

import org.slf4j.Logger;
import org.slf4j.event.Level;
import org.slf4j.spi.LoggingEventBuilder;

public class LogUtils {
    public static LoggingEventBuilder at(final Logger log, final Level level) {
        return log.makeLoggingEventBuilder(level);
    }
}
