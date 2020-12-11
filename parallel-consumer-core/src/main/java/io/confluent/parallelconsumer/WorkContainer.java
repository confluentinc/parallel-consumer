package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.WallClock;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.confluent.csid.utils.KafkaUtils.toTP;

@Slf4j
@EqualsAndHashCode
public class WorkContainer<K, V> implements Comparable<WorkContainer> {

    private final String DEFAULT_TYPE = "DEFAULT";

    /**
     * Simple way to differentiate treatment based on type
     */
    @Getter
    @Setter
    private String workType;

    @Getter
    private final ConsumerRecord<K, V> cr;

    @Getter
    private int numberOfFailedAttempts;

    private Optional<Instant> failedAt = Optional.empty();

    private boolean inFlight = false;

    @Getter
    private Optional<Boolean> userFunctionSucceeded = Optional.empty();

    /**
     * Wait this long before trying again
     */
    private Duration retryDelay;

    @Setter
    static Duration defaultRetryDelay = Duration.ofSeconds(1);

    @Getter
    @Setter(AccessLevel.PUBLIC)
    private Future<List<Object>> future;

    private long timeTakenAsWorkMs;

    public WorkContainer(ConsumerRecord<K, V> cr) {
        this.cr = cr;
        workType = DEFAULT_TYPE;
    }

    public WorkContainer(ConsumerRecord<K, V> cr, String workType) {
        this.cr = cr;
        Objects.requireNonNull(workType);
        this.workType = workType;
    }

    public void fail(WallClock clock) {
        log.trace("Failing {}", this);
        numberOfFailedAttempts++;
        failedAt = Optional.of(clock.getNow());
        inFlight = false;
    }

    public void succeed() {
        log.trace("Succeeded {}", this);
        inFlight = false;
    }

    public boolean hasDelayPassed(WallClock clock) {
        long delay = getDelay(TimeUnit.MILLISECONDS, clock);
        boolean delayHasPassed = delay <= 0;
        return delayHasPassed;
    }

    public long getDelay(TimeUnit unit, WallClock clock) {
        Instant now = clock.getNow();
        Temporal nextAttemptAt = tryAgainAt(clock);
        Duration between = Duration.between(now, nextAttemptAt);
        long convert = unit.convert(between.toMillis(), TimeUnit.MILLISECONDS); // java 8, @see Duration#convert
        return convert;
    }

    private Temporal tryAgainAt(WallClock clock) {
        if (failedAt.isPresent()) {
            Duration retryDelay = getRetryDelay();
            return failedAt.get().plus(retryDelay);
        } else
            return clock.getNow();
    }

    public Duration getRetryDelay() {
        if (retryDelay == null)
            return defaultRetryDelay;
        else
            return retryDelay;
    }

    @Override
    public int compareTo(WorkContainer o) {
        long myOffset = this.cr.offset();
        long theirOffset = o.cr.offset();
        int compare = Long.compare(myOffset, theirOffset);
        return compare;
    }

    public boolean isNotInFlight() {
        return !inFlight;
    }

    public boolean isInFlight() {
        return inFlight;
    }

    public void takingAsWork() {
        log.trace("Being taken as work: {}", this);
        inFlight = true;
        timeTakenAsWorkMs = System.currentTimeMillis();
    }

    public TopicPartition getTopicPartition() {
        return toTP(getCr());
    }

    public void onUserFunctionSuccess() {
        this.userFunctionSucceeded = Optional.of(true);
    }

    public void onUserFunctionFailure() {
        this.userFunctionSucceeded = Optional.of(false);
    }

    public boolean isUserFunctionComplete() {
        return this.getUserFunctionSucceeded().isPresent();
    }

    public boolean isUserFunctionSucceeded() {
        Optional<Boolean> userFunctionSucceeded = this.getUserFunctionSucceeded();
        return userFunctionSucceeded.orElse(false);
    }

    @Override
    public String toString() {
//        return "WorkContainer(" + toTP(cr) + ":" + cr.offset() + ":" + cr.key() + ":" + cr.value() + ")";
        return "WorkContainer(" + toTP(cr) + ":" + cr.offset() + ":" + cr.key() + ")";
    }

    public Duration getTimeInFlight() {
        return Duration.ofMillis(System.currentTimeMillis() - timeTakenAsWorkMs);
    }

    public long offset() {
        return getCr().offset();
    }

    public boolean hasPreviouslyFailed() {
        return getNumberOfFailedAttempts() > 0;
    }
}