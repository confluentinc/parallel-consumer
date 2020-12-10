package io.confluent.parallelconsumer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

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
     */
    private static final int DEFAULT_INITIAL_LOADING_FACTOR = 2;

    private final long startTimeMs = System.currentTimeMillis();
    private final Duration coolDown = Duration.ofSeconds(1);
    private final Duration warmUp = Duration.ofSeconds(0); // CG usually takes 5 seconds to start running
    private final int stepUpFactorBy = 2;

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

    /**
     * Try to increase the loading factor
     *
     * @return true if could step up
     */
    public boolean maybeStepUp() {
        long nowMs = System.currentTimeMillis();
        if (couldStep()) {
            return doStep(nowMs, lastSteppedFactor);
        }
        return false;
    }

    private synchronized boolean doStep(final long nowMs, final long myLastStep) {
        if (currentFactor < maxFactor) {
            // compare and set
            if (myLastStep == lastSteppedFactor) {
                currentFactor = currentFactor + stepUpFactorBy;
                long delta = currentFactor - myLastStep;
                log.debug("Stepped up load factor by {} from {} to {}", delta, myLastStep, currentFactor);
                lastSteppedFactor = currentFactor;
                return true;
            } else {
                // already done
                return false;
            }
        } else {
            return false;
        }
    }

    boolean couldStep() {
        return isWarmUpPeriodOver() && isNoCoolDown();
    }

    private boolean isNoCoolDown() {
        if (lastSteppedFactor == 0) return true;
        long now = System.currentTimeMillis();
        long elapsed = now - lastSteppedFactor;
        return elapsed > coolDown.toMillis();
    }

    boolean isWarmUpPeriodOver() {
        long now = System.currentTimeMillis();
        long elapsed = now - startTimeMs;
        return elapsed > warmUp.toMillis();
    }

    public boolean isMaxReached() {
        return currentFactor >= maxFactor;
    }
}
