
/*-
 * Copyright (C) 2020 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests.sanity;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.DelegatingProgressBarConsumer;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Testing use of progres bar in different console environments (e.g. intellij)
 */
@Slf4j
public class ProgressBarTest {

    @SneakyThrows
    @Test
    @Disabled("For reference sanity only")
    public void width() {
        DelegatingProgressBarConsumer delegatingProgressBarConsumer = new DelegatingProgressBarConsumer(log::info);

        ProgressBar build = new ProgressBarBuilder().setConsumer(delegatingProgressBarConsumer).setInitialMax(100).showSpeed().setTaskName("progress").setUnit("msg", 1).build();
        try (build) {
            while (build.getCurrent() < build.getMax()) {
                build.step();
                Thread.sleep(100);
            }
        }
    }
}
