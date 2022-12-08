package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.csid.utils.Range.range;
import static io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase.DEFAULT_TIMEOUT_SECONDS;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class LatchTestUtils {

    @SneakyThrows
    public static void awaitLatch(List<CountDownLatch> latches, int latchIndex) {
        log.trace("Waiting on latch {}", latchIndex);
        try {
            awaitLatchWithException(latches.get(latchIndex));
        } catch (TimeoutException e) {
            log.error("Waiting on latch {} timed out!", latchIndex);
            throw e;
        }
        log.trace("Wait on latch {} finished.", latchIndex);
    }

    @SneakyThrows
    public static void awaitLatch(CountDownLatch latch) {
        awaitLatch(latch, DEFAULT_TIMEOUT_SECONDS);
    }

    public static void awaitLatchWithException(CountDownLatch latch) throws TimeoutException {
        // latch timeouts should be longer, as they are never the root cause, and so can hide the root if time out too soone
        awaitLatch(latch, DEFAULT_TIMEOUT_SECONDS * 2);
    }

    //    @SneakyThrows
    public static void awaitLatch(final CountDownLatch latch, final int seconds) throws TimeoutException {
        log.trace("Waiting on latch with timeout {}s", seconds);
        Instant start = now();
        boolean latchReachedZero = false;
        while (start.isAfter(now().minusSeconds(seconds))) {
            try {
                latchReachedZero = latch.await(seconds, SECONDS);
            } catch (InterruptedException e) {
                log.trace("Latch await interrupted", e);
            }
            if (latchReachedZero)
                break;
            else
                log.trace("Latch wait aborted, but timeout ({}s) not reached - will try to wait again remaining {}",
                        seconds, seconds - toSeconds(between(start, now())));
        }
        if (latchReachedZero) {
            log.trace("Latch was released (#countdown)");
        } else {
            throw new TimeoutException("Latch await timeout (" + seconds + " seconds) - " + latch.getCount() + " count remaining");
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
