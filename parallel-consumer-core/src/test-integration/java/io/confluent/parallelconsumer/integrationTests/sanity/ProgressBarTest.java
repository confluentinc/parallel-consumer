
/*-
 * Copyright (C) 2020 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests.sanity;

import io.confluent.csid.utils.ProgressBarUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
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
        ProgressBar build = ProgressBarUtils.getNewMessagesBar(log, 100);
        try (build) {
            while (build.getCurrent() < build.getMax()) {
                build.step();
                Thread.sleep(100);
            }
        }
    }
}
