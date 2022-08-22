package io.confluent.csid.utils;

import com.google.common.truth.Truth;
import org.apache.commons.lang3.NotImplementedException;

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

        await("pretend to start to commit - acquire commit lock")
                .pollDelay(blockedForAtLeast) // makes sure it is still blocked after 1 second
                .atMost(blockedForAtLeast.plus(Duration.ofSeconds(1)))
                .untilAsserted(
                        () -> Truth.assertWithMessage("Thread should be sleeping/blocked and not have returned")
                                .that(methodReturned.get())
                                .isFalse());
    }

    // todo: method: assert function doesn't block. Note on JUnit's technique about it not always working.

    // todo method: assert function unblocks only after 2 seconds - useful for when we can't use a separate thread to check due to locking semantics
    public void assertUnblocksAfter(final Runnable functionExpectedToBlock,
                                    final Duration unblocksAfter) {
        throw new NotImplementedException();
    }
}
