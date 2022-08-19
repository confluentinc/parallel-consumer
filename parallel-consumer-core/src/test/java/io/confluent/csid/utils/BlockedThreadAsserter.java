package io.confluent.csid.utils;

import com.google.common.truth.Truth;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public class BlockedThreadAsserter {

    AtomicBoolean methodReturned = new AtomicBoolean(false);

    public boolean functionHasCompleted() {
        return methodReturned.get();
    }

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

        await("pretend to start to commit - acquire commit lock")
                .pollDelay(blockedForAtLeast) // makes sure it is still blocked after 1 second
                .untilAsserted(
                        () -> Truth.assertWithMessage("Thread should be sleeping/blocked and not have returned")
                                .that(methodReturned.get())
                                .isFalse());
    }
}
