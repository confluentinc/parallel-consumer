package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
import io.confluent.parallelconsumer.internal.InternalRuntimeError;
import lombok.Getter;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase.defaultTimeout;

/**
 * Used to check that progress has been made in some activity.
 */
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

    @Getter
    private Duration timeout = defaultTimeout;

    private Integer roundsAllowed = 10;

    private final int coldRoundsAllowed = 20;

    @Getter
    private int highestRoundCountSeen = 0;
    private final Instant startTime = Instant.now();

    public ProgressTracker(final AtomicInteger processedCount, Integer roundsAllowed, Duration timeout) {
        this.processedCount = processedCount;
        if (roundsAllowed != null && timeout != null)
            throw new IllegalArgumentException("Can't provide both a timeout and a number of rounds");
        this.roundsAllowed = roundsAllowed;
        this.timeout = timeout;
    }

    public ProgressTracker(final AtomicInteger processedCount) {
        this.processedCount = processedCount;
    }

    /**
     * Checks progress has been made. Increments the count of rounds / checks.
     *
     * @return false if progress has been made, true otherwise
     */
    public boolean hasProgressNotBeenMade() {
        boolean progress = processedCount.get() > lastSeen.get();
        boolean warmedUp = processedCount.get() > WARMED_UP_AFTER_X_MESSAGES;
        boolean enoughAttempts = hasTimeoutPassed();
        if (warmedUp && !progress && enoughAttempts) {
            return true;
        } else if (!warmedUp && this.roundsAllowed != null && rounds.get() > coldRoundsAllowed) {
            return true;
        } else if (progress) {
            reset();
        }
        lastSeen.set(processedCount.get());
        rounds.incrementAndGet();
        return false;
    }

    private boolean hasTimeoutPassed() {
        // in the case both are present, prefer rounds to duration (legacy)
        if (roundsAllowed != null) {
            return rounds.get() > roundsAllowed;
        } else {
            Duration remainingTime = Duration.between(Instant.now(), getDeadline());
            return remainingTime.isNegative();
        }
    }

    private Temporal getDeadline() {
        return startTime.plus(timeout);
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
        boolean noProgress = hasProgressNotBeenMade();
        if (noProgress)
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
