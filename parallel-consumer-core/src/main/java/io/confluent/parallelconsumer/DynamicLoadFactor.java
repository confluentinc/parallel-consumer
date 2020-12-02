package io.confluent.parallelconsumer;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.time.Duration;

@RequiredArgsConstructor
public class DynamicLoadFactor {

    private final long start = System.currentTimeMillis();
    private final Duration coolDown = Duration.ofSeconds(2);
    private final Duration warmUp = Duration.ofSeconds(5 + 3); // CG usually takes 5 seconds to start running
    private long lastStep = 0;
    private final int step = 1;

    @Getter
    int current = 0;

    public boolean maybeIncrease() {
        long now = System.currentTimeMillis();
        if (couldStep()) {
            return doStep(now, lastStep);
        }
        return false;
    }

    private synchronized boolean doStep(final long now, final long myLastStep) {
        // compare and set
        if (myLastStep == lastStep) {
            current = current + step;
            lastStep = now;
            return true;
        } else {
            // already done
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
}
