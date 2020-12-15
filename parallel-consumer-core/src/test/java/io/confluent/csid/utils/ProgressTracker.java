package io.confluent.csid.utils;

import io.confluent.parallelconsumer.InternalRuntimeError;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.StringUtils.msg;

/**
 * Used to check that progress has been made in some activity.
 */
@RequiredArgsConstructor
public class ProgressTracker {

    public static final int WARMED_UP_AFTER_X_MESSAGES = 50;

    /**
     * The shared count of progress.
     */
    private final AtomicInteger processedCount;

    private final AtomicInteger lastSeen = new AtomicInteger(0);

    /**
     * How many times progress has been checked for.
     */
    @Getter
    private final AtomicInteger rounds = new AtomicInteger(0);

    private int roundsAllowed = 3;

    private int coldRoundsAllowed = 20;

    @Getter
    private int highestRoundCountSeen = 0;

    public ProgressTracker(final AtomicInteger processedCount, int roundsAllowed) {
        this.processedCount = processedCount;
        this.roundsAllowed = roundsAllowed;
    }

    /**
     * Checks progress has been made. Increments the count of rounds / checks.
     *
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
            reset();
        }
        lastSeen.set(processedCount.get());
        rounds.incrementAndGet();
        return false;
    }

    private void reset() {
        if (rounds.get() > highestRoundCountSeen)
            highestRoundCountSeen = rounds.get();
        rounds.set(0);
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
        return constructError("");
    }

    public Exception constructError(String messageToAppend) {
        return new InternalRuntimeError(msg("No progress beyond {} records after {} rounds. {}",
                processedCount, rounds, messageToAppend));
    }
}
