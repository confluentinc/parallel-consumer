package io.confluent.csid.utils;

import com.google.common.truth.Truth;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
@Slf4j
public class BlockedThreadAsserter {

    AtomicBoolean methodReturned = new AtomicBoolean(false);

    public boolean functionHasCompleted() {
        return methodReturned.get();
    }

    /**
     * todo add string message param
     */
    public void assertFunctionBlocks(Runnable functionExpectedToBlock, final Duration blockedForAtLeast) {
        Thread blocked = new Thread(() -> {
            try {
                functionExpectedToBlock.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
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

    // todo: method: assert function doesn't block. Note on JUnit's technique about it not always working.

    // todo method: assert function unblocks only after 2 seconds - useful for when we can't use a separate thread to check due to locking semantics
    final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public void assertUnblocksAfter(final Runnable functionExpectedToBlock,
                                    final Runnable unblockingFunction,
                                    final Duration unblocksAfter) {

        AtomicBoolean unblockerHasRun = new AtomicBoolean(false);
        scheduledExecutorService.schedule(() -> {
                    log.debug("Running unblocking function - blocked function should return ONLY after this (which will be tested)");
                    unblockingFunction.run();
                    unblockerHasRun.set(true);
                },
                unblocksAfter.toMillis(),
                TimeUnit.MILLISECONDS);

        var time = TimeUtils.timeWithMeta(() -> {
            log.debug("Running function expected to block for {}...", unblocksAfter);
            functionExpectedToBlock.run();
            return Void.class;
        });
        log.debug("Function unblocked after {}", time.getElapsed());

        this.methodReturned.set(true);

        Truth.assertThat(time.getElapsed()).isAtLeast(unblocksAfter);
        Truth.assertWithMessage("Unblocking function should complete OK (if false, may not have run at all - or that the expected function to block did NOT block)")
                .that(unblockerHasRun.get()).isTrue();
    }
}
