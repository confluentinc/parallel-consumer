package io.confluent.parallelconsumer.sanity;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
class InterruptionTests {

    /**
     * Verify behaviour of 0 vs 1 timeout on {@link Object@wait}. Original test timeout of 5ms was too small, sometimes
     * (1/4000) runs it would timeout. 1/117,000 it failed at 50ms. 1 second didn't observe failure within ~250,000 runs
     * in Intellij (run until fail).
     */
    @Timeout(value = 1, unit = SECONDS)
    @SneakyThrows
    @Test
    void waitOnZeroCausesInfiniteWait() {
        Object lock = new Object();
        try {
            synchronized (lock) {
                lock.wait(1);
                // lock.wait(0); // zero causes it to wait forever
            }
        } catch (InterruptedException e) {
            log.debug("", e);
        }
    }
}
