package io.confluent.csid.utils;

import lombok.experimental.UtilityClass;
import me.tongfei.progressbar.DelegatingProgressBarConsumer;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.slf4j.Logger;

@UtilityClass
public class ProgressBarUtils {
    public static ProgressBar getNewMessagesBar(Logger log, int initialMax) {
        ProgressBar build = getOptions(log, initialMax).build();
        return build;
    }

    public static ProgressBarBuilder getOptions(Logger log, int initialMax) {
        DelegatingProgressBarConsumer delegatingProgressBarConsumer = new DelegatingProgressBarConsumer(log::info);
        ProgressBarBuilder build = new ProgressBarBuilder()
                .setConsumer(delegatingProgressBarConsumer)
                .setInitialMax(initialMax)
                .showSpeed()
                .setTaskName("progress")
                .setUnit("msg", 1);
        return build;
    }
}
