package io.confluent.csid.testcontainers;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.event.Level;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Filters out some log levels from the test container (e.g. Kafka's container has TRACE level set by default).
 * <p>
 * Enable logging for this class to get log statements from docker.
 */
@Slf4j // internal logger
public class FilteredTestContainerSlf4jLogConsumer extends Slf4jLogConsumer {

    /**
     * Logger to send followed logs to
     */
    private final Logger loggerToUse;

    private String prefix;

    public FilteredTestContainerSlf4jLogConsumer(Logger logger) {
        this(logger, false);
    }

    public FilteredTestContainerSlf4jLogConsumer(Logger logger, boolean separateOutputStreams) {
        super(logger, separateOutputStreams);
        this.loggerToUse = logger;
    }

    @Override
    public FilteredTestContainerSlf4jLogConsumer withPrefix(final String prefix) {
        super.withPrefix(prefix);
        this.prefix = prefix;
        return this;
    }

    @Override
    public void accept(OutputFrame outputFrame) {
        if (loggerToUse.isDebugEnabled()) {
            try {
                logWithLevelPassThrough(outputFrame);
            } catch (Exception e) {
                log.error("Failed to log output frame", e);
            }
        }
    }

    private void logWithLevelPassThrough(OutputFrame outputFrame) {
        OutputFrame.OutputType outputType = outputFrame.getType();

        String utf8String = outputFrame.getUtf8String();

        var level = extractLevelFromLogString(utf8String);

        if (level.isEmpty()) {
            super.accept(outputFrame);
            return;
        }

        switch (outputType) {
            case END:
                break;
            case STDOUT:
            case STDERR:
                loggerToUse.atLevel(level.get()).log("{}{}", prefix.isEmpty() ? "" : (prefix + ": "), StringUtils.chomp(utf8String));
                break;
            default:
                throw new IllegalArgumentException("Unexpected outputType " + outputType);
        }
    }

    private Optional<Level> extractLevelFromLogString(String logString) {
        if (logString.isBlank())
            return Optional.empty();

        Map<Level, Integer> levelScores = new HashMap<>();


        for (Level l : Level.values()) {
            levelScores.put(l, findIndexOfLevel(logString, l));
        }

        // lowest score wins, unless it's less than zero
        var maxEntry = levelScores.entrySet().stream()
                .filter(levelIntegerEntry -> levelIntegerEntry.getValue() >= 0)
                .min(Map.Entry.comparingByValue()); // first position is lowest

        if (maxEntry.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(maxEntry.get().getKey());
    }

    private int findIndexOfLevel(String logString, Level level) {
        String levelString = level.toString().toLowerCase();
        return logString.toLowerCase().indexOf(levelString);
    }

}
