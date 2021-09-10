package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static io.confluent.csid.utils.Range.range;
import static io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase.defaultTimeout;
import static io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase.defaultTimeoutSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class LatchTestUtils {

    public static void awaitLatch(List<CountDownLatch> latches, int latchIndex) {
        log.trace("Waiting on latch {}", latchIndex);
        awaitLatch(latches.get(latchIndex));
    }

    @SneakyThrows
    public static void awaitLatch(CountDownLatch latch) {
        awaitLatch(latch, defaultTimeoutSeconds);
    }

    @SneakyThrows
    public static void awaitLatch(final CountDownLatch latch, final int seconds) {
        log.trace("Waiting on latch with timeout {}", defaultTimeout);
        boolean latchReachedZero = latch.await(seconds, SECONDS);
        if (latchReachedZero) {
            log.trace("Latch released");
        } else {
            throw new AssertionError("Latch await timeout - " + latch.getCount() + " remaining");
        }
    }

    public static void release(List<CountDownLatch> locks, int lockIndex) {
        log.debug("Releasing {}...", lockIndex);
        locks.get(lockIndex).countDown();
    }

    public static List<CountDownLatch> constructLatches(int numberOfLatches) {
        var result = new ArrayList<CountDownLatch>(numberOfLatches);
        for (var ignore : range(numberOfLatches)) {
            result.add(new CountDownLatch(1));
        }
        return result;
    }

    public static void release(final CountDownLatch latch) {
        log.info("Latch countdown");
        latch.countDown();
    }
}
