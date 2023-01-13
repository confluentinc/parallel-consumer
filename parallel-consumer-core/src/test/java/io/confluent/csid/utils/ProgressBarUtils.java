package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.experimental.UtilityClass;
import me.tongfei.progressbar.DelegatingProgressBarConsumer;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.slf4j.Logger;

@UtilityClass
public class ProgressBarUtils {

    private static final String DEFAULT_LABEL = "~";

    public static ProgressBar getNewMessagesBar(Logger log, long initialMax) {
        return getNewMessagesBar(null, log, initialMax);
    }

    public static ProgressBar getNewMessagesBar(String name, Logger log, long initialMax) {
        DelegatingProgressBarConsumer delegatingProgressBarConsumer = new DelegatingProgressBarConsumer(log::info);

        if (StringUtils.isBlank(name))
            name = DEFAULT_LABEL;

        return new ProgressBarBuilder()
                .setConsumer(delegatingProgressBarConsumer)
                .setInitialMax(initialMax)
                .showSpeed()
                .setTaskName(name)
                .setUnit("msg", 1)
                .build();
    }
}
