package io.confluent.parallelconsumer;

import lombok.SneakyThrows;

import java.time.Duration;
import java.util.concurrent.Callable;

public class RateLimiter {

    private Duration rate = Duration.ofSeconds(1);
    private long lastFire = 0;

    public RateLimiter() {
    }

    public RateLimiter(int seconds) {
        this.rate = Duration.ofSeconds(seconds);
    }

    @SneakyThrows
    public void limit(final Runnable action) {
        if (isOkToCallAction()) {
            lastFire = System.currentTimeMillis();
            action.run();
        }
    }

    private boolean isOkToCallAction() {
        long now = System.currentTimeMillis();
        long elapsed = now - lastFire;
        return lastFire == 0 || elapsed > rate.toMillis();
    }

}
