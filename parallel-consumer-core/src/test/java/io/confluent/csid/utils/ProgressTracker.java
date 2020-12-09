package io.confluent.csid.utils;

import io.confluent.parallelconsumer.InternalRuntimeError;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.StringUtils.msg;

@RequiredArgsConstructor
public class ProgressTracker {

    public static final int WARMED_UP_AFTER_X_MESSAGES = 50;
    private final AtomicInteger processedCount;
    private final AtomicInteger lastSeen = new AtomicInteger(0);
    private final AtomicInteger rounds = new AtomicInteger(0);
    private int roundsAllowed = 3;
    private int coldRoundsAllowed = 20;

    public ProgressTracker(final AtomicInteger processedCount, int roundsAllowed) {
        this.processedCount = processedCount;
        this.roundsAllowed = roundsAllowed;
    }

    /**
     * @return false if progress has been made, true otherwise
     */
    public boolean hasProgressNotBeenMade() {
        boolean progress = processedCount.get() > lastSeen.get();
        boolean warmedUp = processedCount.get() > WARMED_UP_AFTER_X_MESSAGES;
        boolean enoughAttempts = rounds.get() > roundsAllowed;
        if (warmedUp && !progress && enoughAttempts) {
            return true;
        } else if (!warmedUp && rounds.get() > coldRoundsAllowed) {
            return true;
        } else if (progress) {
            rounds.set(0);
        }
        lastSeen.set(processedCount.get());
        rounds.incrementAndGet();
        return false;
    }

    /**
     * @throws Exception If no progress is made
     */
    public void checkForProgressExceptionally() throws Exception {
        boolean progress = hasProgressNotBeenMade();
        if (progress)
            throw constructError();
    }

    public Exception constructError() {
        return new InternalRuntimeError(msg("No progress beyond {} records after {} rounds", processedCount, rounds));
    }
}
