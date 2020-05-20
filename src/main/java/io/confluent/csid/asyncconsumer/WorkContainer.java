package io.confluent.csid.asyncconsumer;

import io.confluent.csid.utils.WallClock;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Data
public class WorkContainer<K, V> implements Comparable<WorkContainer> {

    private final ConsumerRecord<K, V> cr;
    private int numberOfAttempts;
    private Object latestFutureResult;
    private Optional<Instant> failedAt = Optional.empty();
    private boolean inFlight = false;

    @Getter
    private static Duration retryDelay = Duration.ofSeconds(10);

    @Getter
    @Setter
    private Future<ProducerRecord<K, V>> future;

    public void fail(WallClock clock) {
        numberOfAttempts++;
        failedAt = Optional.of(clock.getNow());
        inFlight = false;
    }

    public void succeed() {
        inFlight = false;
    }

    public boolean hasDelayPassed(WallClock clock) {
        long delay = getDelay(TimeUnit.SECONDS, clock);
        boolean delayHasPassed = delay <= 0;
        return delayHasPassed;
    }

    //    @Override
    public long getDelay(@NotNull TimeUnit unit, WallClock clock) {
        Instant now = clock.getNow();
        Duration between = Duration.between(now, tryAgainAt(clock));
        long convert = unit.convert(between);
        return convert;
    }

    private Temporal tryAgainAt(WallClock clock) {
        if (failedAt.isPresent())
            return failedAt.get().plus(retryDelay);
        else
            return clock.getNow();
    }

    @Override
    public int compareTo(@NotNull WorkContainer o) {
        long myOffset = this.cr.offset();
        long theirOffset = o.cr.offset();
        int compare = Long.compare(myOffset, theirOffset);
        return compare;
    }

    public boolean isNotInFlight() {
        return !inFlight;
    }

    public void takingAsWork() {
        inFlight = true;
    }
}