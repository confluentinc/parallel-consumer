package io.confluent.parallelconsumer;

import lombok.Getter;

import java.time.Duration;

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

    private final long start = System.currentTimeMillis();
    private final Duration coolDown = Duration.ofSeconds(2);
    private final Duration warmUp = Duration.ofSeconds(5); // CG usually takes 5 seconds to start running
    private long lastStep = 0;
    private final int step = 1;
    @Getter
    private final int max = 5;

    @Getter
    int current = DEFAULT_INITIAL_LOADING_FACTOR;

    public boolean maybeStepUp() {
        long now = System.currentTimeMillis();
        if (couldStep()) {
            return doStep(now, lastStep);
        }
        return false;
    }

    private synchronized boolean doStep(final long now, final long myLastStep) {
        if (current < max) {
            // compare and set
            if (myLastStep == lastStep) {
                current = current + step;
                lastStep = now;
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
        if (lastStep == 0) return true;
        long now = System.currentTimeMillis();
        long elapsed = now - lastStep;
        return elapsed > coolDown.toMillis();
    }

    boolean isWarmUpPeriodOver() {
        long now = System.currentTimeMillis();
        long elapsed = now - start;
        return elapsed > warmUp.toMillis();
    }

    public boolean isMaxReached() {
        return current >= max;
    }
}
