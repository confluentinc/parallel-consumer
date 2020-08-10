package io.confluent.csid.testcontainers;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.event.Level;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.List;

import static org.slf4j.event.Level.DEBUG;
import static org.slf4j.event.Level.TRACE;

/**
 * Filters out some log levels from the test container (e.g. Kafka's container has TRACE level set by default).
 */
public class FilteredSlf4jLogConsumer extends Slf4jLogConsumer {

    @Getter
    @Setter
    private List<Level> filteredLevels = List.of(TRACE, DEBUG);

    public FilteredSlf4jLogConsumer(Logger logger) {
        super(logger);
    }

    public FilteredSlf4jLogConsumer(Logger logger, boolean separateOutputStreams) {
        super(logger, separateOutputStreams);
    }

    @Override
    public void accept(OutputFrame outputFrame) {
        String utf8String = outputFrame.getUtf8String();
        boolean isFilteredOut = filteredLevels.stream().anyMatch(level -> utf8String.contains(level.toString()));
        if (!isFilteredOut) {
            super.accept(outputFrame);
        } else {
            // ignoring trace level logging
        }
    }
}
