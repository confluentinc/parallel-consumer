package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;

/**
 * Controls a loading factor. Is used to ensure enough messages in multiples of our target concurrency are queued ready
 * for processing.
 * <p>
 * Ensures that increases in loading factor aren't performed a) too soon after the last increase ({@link
 * #isNotCoolingDown()})  and b) too soon after starting the system ({@link #isWarmUpPeriodOver()}).
 */
// todo make so can be fractional like 50% - this is because some systems need a fractional factor, like 1.1 or 1.2 rather than 2
@Slf4j
public class DynamicLoadFactor {

    /**
     * Don't change this unless you know what you're doing.
     * <p>
     * The default value is already quite aggressive for very fast processing functions.
     * <p>
     * This controls the loading factor of the buffers used to feed the executor engine. A higher value means more
     * memory usage, but more importantly, more offsets may be beyond the highest committable offset for processing
     * (which if the serialised information can't fit, will be dropped and could cause much larger replays than
     * necessary).
     * <p>
     * Starts off as 2, so there's a good guarantee that we start off with enough data queued up.
     */
    private static final int DEFAULT_INITIAL_LOADING_FACTOR = 2;

    private final long startTimeMs = System.currentTimeMillis();

    /**
     * Min duration between steps - i.e. don't ever step up faster than this
     */
    private final Duration coolDown = Duration.ofSeconds(2);

    /**
     * Delay after initial start, before any steps are allowed.
     * <p>
     * Consumer Group usually takes a few seconds to start running
     */
    private final Duration warmUp = Duration.ofSeconds(2);

    /**
     * The amount to increase the current load by, each step
     */
    private final int stepUpFactorBy = 1;

    /**
     * Upper safety cap on multiples of target queue size to reach (e.g. with 20 threads, this would be 20 * 100 =
     * 20,000 messages _queued_.
     * <p>
     * Expectation is some relatively small multiple of the degree of concurrency, enough that each time a thread
     * finishes, theres at least one more entry for it in the queue.
     */
    @Getter
    private final int maxFactor = 100;

    @Getter
    private int currentFactor = DEFAULT_INITIAL_LOADING_FACTOR;
    private long lastSteppedFactor = currentFactor;
    private Instant lastStepTime = Instant.MIN;

    /**
     * Try to increase the loading factor
     *
     * @return true if could step up
     */
    public boolean maybeStepUp() {
        if (couldStep()) {
            return doStep();
        }
        return false;
    }

    private synchronized boolean doStep() {
        if (isMaxReached()) {
            return false;
        } else {
            // compare and set
            currentFactor = currentFactor + stepUpFactorBy;
            long delta = currentFactor - lastSteppedFactor;
            log.debug("Stepped up load factor by {} from {} to {}", delta, lastSteppedFactor, currentFactor);

            //
            lastSteppedFactor = currentFactor;
            lastStepTime = Instant.now();
            return true;
        }
    }

    /**
     * Checks various conditions to see if a step is allowed
     *
     * @return true is a step-up is now allowed
     */
    boolean couldStep() {
        boolean warmUpPeriodOver = isWarmUpPeriodOver();
        boolean noCoolDown = isNotCoolingDown();
        return warmUpPeriodOver && noCoolDown;
    }

    /**
     * @return true if the cool-down period is over
     * @see #coolDown
     */
    private boolean isNotCoolingDown() {
        var now = Instant.now();
        Duration elapsed = Duration.between(lastStepTime, now);
        boolean coolDownElapsed = elapsed.compareTo(coolDown) > 0;
        return coolDownElapsed;
    }

    /**
     * Is the warm-up period over?
     *
     * @return true if warn up os over
     * @see #warmUp
     */
    public boolean isWarmUpPeriodOver() {
        long now = System.currentTimeMillis();
        long elapsed = now - startTimeMs;
        return elapsed > warmUp.toMillis();
    }

    public boolean isMaxReached() {
        return currentFactor >= maxFactor;
    }
}
