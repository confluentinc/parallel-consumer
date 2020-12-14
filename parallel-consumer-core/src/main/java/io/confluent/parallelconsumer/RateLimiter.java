package io.confluent.parallelconsumer;

import lombok.Getter;
import lombok.SneakyThrows;

import java.time.Duration;

public class RateLimiter {

    @Getter
    private Duration rate = Duration.ofSeconds(1);
    private long lastFireMs = 0;

    public RateLimiter() {
    }

    public RateLimiter(int seconds) {
        this.rate = Duration.ofSeconds(seconds);
    }

    @SneakyThrows
    public void performIfNotLimited(final Runnable action) {
        if (isOkToCallAction()) {
            lastFireMs = System.currentTimeMillis();
            action.run();
        }
    }

    public boolean couldPerform() {
        return isOkToCallAction();
    }

    private boolean isOkToCallAction() {
        long elapsed = getElapsedMs();
        return lastFireMs == 0 || elapsed > rate.toMillis();
    }

    private long getElapsedMs() {
        long now = System.currentTimeMillis();
        long elapsed = now - lastFireMs;
        return elapsed;
    }

    public Duration getElapsedDuration() {
        return Duration.ofMillis(getElapsedMs());
    }

}
