package io.confluent.csid.utils;

import io.confluent.parallelconsumer.InternalRuntimeError;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.StringUtils.msg;

@RequiredArgsConstructor
public class ProgressTracker {

    private final AtomicInteger processedCount;
    private final AtomicInteger lastSeen = new AtomicInteger(0);
    private final AtomicInteger rounds = new AtomicInteger(0);
    private final int roundsAllowed = 3;
    private final int coldRoundsAllowed = 20;

    /**
     * @return true if progress has been made, false if not
     */
    public boolean checkForProgress() {
        boolean progress = processedCount.get() > lastSeen.get();
        boolean warmedUp = processedCount.get() > 0;
        boolean enoughAttempts = rounds.get() > roundsAllowed;
        if (warmedUp && !progress && enoughAttempts) {
            return true;
        } else if (progress) {
            rounds.set(0);
        } else if (!warmedUp && rounds.get() > coldRoundsAllowed) {
            return true;
        }
        lastSeen.set(processedCount.get());
        rounds.incrementAndGet();
        return false;
    }

    public void checkForProgressExceptionally() {
        boolean progress = checkForProgress();
        if (!progress) throw new InternalRuntimeError("No progress");
    }

    public Exception getError() {
        return new InternalRuntimeError(msg("No progress beyond {} records after {} rounds", processedCount, rounds));
    }
}
