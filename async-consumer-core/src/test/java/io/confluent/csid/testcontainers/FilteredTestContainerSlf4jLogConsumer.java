package io.confluent.csid.testcontainers;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.event.Level;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;

import static org.slf4j.event.Level.DEBUG;
import static org.slf4j.event.Level.TRACE;

/**
 * Filters out some log levels from the test container (e.g. Kafka's container has TRACE level set by default).
 *
 * Enable logging for this class to get log statements from docker.
 */
@Slf4j
public class FilteredTestContainerSlf4jLogConsumer extends Slf4jLogConsumer {

    @Getter
    @Setter
    private List<Level> filteredLevels = UniLists.of(TRACE, DEBUG);

    public FilteredTestContainerSlf4jLogConsumer(Logger logger) {
        super(logger);
    }

    public FilteredTestContainerSlf4jLogConsumer(Logger logger, boolean separateOutputStreams) {
        super(logger, separateOutputStreams);
    }

    @Override
    public void accept(OutputFrame outputFrame) {
        if (log.isDebugEnabled()) {
            String utf8String = outputFrame.getUtf8String();
            boolean isFilteredOut = filteredLevels.stream().anyMatch(level -> utf8String.contains(level.toString()));
            if (!isFilteredOut) {
                super.accept(outputFrame);
            } else {
                // ignoring trace level logging
            }
        }
    }
}
