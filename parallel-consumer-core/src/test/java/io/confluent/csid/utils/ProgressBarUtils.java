package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import lombok.experimental.UtilityClass;
import me.tongfei.progressbar.DelegatingProgressBarConsumer;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.slf4j.Logger;

@UtilityClass
public class ProgressBarUtils {

    public static ProgressBar getNewMessagesBar(Logger log, int initialMax) {
        return getNewMessagesBar(null, log, initialMax);
    }

    public static ProgressBar getNewMessagesBar(String name, Logger log, int initialMax) {
        DelegatingProgressBarConsumer delegatingProgressBarConsumer = new DelegatingProgressBarConsumer(log::info);

        String usedName = "progress";
        if (name != null)
            usedName = name;

        ProgressBar build = new ProgressBarBuilder()
                .setConsumer(delegatingProgressBarConsumer)
                .setInitialMax(initialMax)
                .showSpeed()
                .setTaskName(usedName)
                .setUnit("msg", 1)
                .build();
        return build;
    }
}
