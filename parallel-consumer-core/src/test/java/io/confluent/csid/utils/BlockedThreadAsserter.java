package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;

/**
 * System for asserting that a given method blocks for some period of time, and optionally unblocks.
 * <p>
 * JUnit has {@link org.junit.jupiter.api.Assertions#assertTimeoutPreemptively} which is useful but has limitations.
 *
 * @author Antony Stubbs
 */
@Slf4j
public class BlockedThreadAsserter {

    /**
     * Could do this faster with a {@link java.util.concurrent.CountDownLatch}
     */
    private final AtomicBoolean methodReturned = new AtomicBoolean(false);

    public boolean functionHasCompleted() {
        return methodReturned.get();
    }


    public void assertFunctionBlocks(Runnable functionExpectedToBlock) {
        assertFunctionBlocks(functionExpectedToBlock, ofSeconds(1));
    }

    public void assertFunctionBlocks(Runnable functionExpectedToBlock, final Duration blockedForAtLeast) {
        Thread blocked = new Thread(() -> {
            try {
                log.debug("Running function expected to block for at least {}...", blockedForAtLeast);
                functionExpectedToBlock.run();
                log.debug("Blocked function finished.");
            } catch (Exception e) {
                log.error("Error in blocking function", e);
            }
            methodReturned.set(true);
        });
        blocked.start();

        await()
                .pollDelay(blockedForAtLeast) // makes sure it is still blocked after 1 second
                .atMost(blockedForAtLeast.plus(Duration.ofSeconds(1)))
                .untilAsserted(
                        () -> Truth.assertWithMessage("Thread should be sleeping/blocked and not have returned")
                                .that(methodReturned.get())
                                .isFalse());
    }

    final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public void assertUnblocksAfter(final Runnable functionExpectedToBlock,
                                    final Runnable unblockingFunction,
                                    final Duration unblocksAfter) {

        AtomicBoolean unblockerHasRun = new AtomicBoolean(false);
        scheduledExecutorService.schedule(() -> {
                    log.debug("Running unblocking function - blocked function should return ONLY after this (which will be tested)");
                    try {
                        unblockingFunction.run();
                    } catch (Exception e) {
                        log.error("Error in unlocking function", e);
                    }
                    unblockerHasRun.set(true);
                    log.debug("Blocked function returned");
                },
                unblocksAfter.toMillis(),
                TimeUnit.MILLISECONDS);

        var time = TimeUtils.timeWithMeta(() -> {
            log.debug("Running function expected to block for at least {}...", unblocksAfter);
            try {
                functionExpectedToBlock.run();
            } catch (Exception e) {
                log.error("Error in blocking function", e);
            }
            log.debug("Unblocking function finished returned");
            return Void.class;
        });
        log.debug("Function unblocked after {}", time.getElapsed());

        this.methodReturned.set(true);

        Truth.assertThat(time.getElapsed()).isAtLeast(unblocksAfter);
        Truth.assertWithMessage("Unblocking function should complete OK (if false, may not have run at all - or that the expected function to block did NOT block)")
                .that(unblockerHasRun.get()).isTrue();
    }

    public void assertUnblocksAfter(final Runnable functionExpectedToBlock,
                                    final Runnable unblockingFunction) {
        assertUnblocksAfter(functionExpectedToBlock, unblockingFunction, ofSeconds(1));
    }

    public void awaitReturnFully() {
        log.debug("Waiting for blocked method to fully finish...");
        await().untilTrue(this.methodReturned);
        log.debug("Waiting on blocked method to fully finish is complete.");
    }
}
