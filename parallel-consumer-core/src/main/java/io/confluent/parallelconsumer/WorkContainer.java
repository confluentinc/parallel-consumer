package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.StringUtils;
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

    private static final String DEFAULT_TYPE = "DEFAULT";

    /**
     * Assignment generation this record comes from. Used for fencing messages after partition loss, for work lingering
     * in the system of in flight.
     */
    @Getter
    private final int epoch;

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

    //    @Getter
    private Optional<Boolean> userFunctionSucceeded = Optional.empty();

    /**
     * Wait this long before trying again
     */
    @Getter
    private static Duration retryDelay = Duration.ofSeconds(10);

    @Getter
    @Setter(AccessLevel.PUBLIC)
    private Future<List<Object>> future;
    private long timeTakenAsWorkMs;

    public WorkContainer(int epoch, ConsumerRecord<K, V> cr) {
        this(epoch, cr, DEFAULT_TYPE);
    }

    public WorkContainer(int epoch, ConsumerRecord<K, V> cr, String workType) {
        this.epoch = epoch;
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
        long delay = getDelay(TimeUnit.SECONDS, clock);
        boolean delayHasPassed = delay <= 0;
        return delayHasPassed;
    }

    public long getDelay(TimeUnit unit, WallClock clock) {
        Instant now = clock.getNow();
        Duration between = Duration.between(now, tryAgainAt(clock));
        long convert = unit.convert(between.toMillis(), TimeUnit.MILLISECONDS); // java 8, @see Duration#convert
        return convert;
    }

    private Temporal tryAgainAt(WallClock clock) {
        if (failedAt.isPresent())
            return failedAt.get().plus(retryDelay);
        else
            return clock.getNow();
    }

    /**
     * @return compares by offset
     */
    @Override
    public int compareTo(WorkContainer workToCompare) {
        long myOffset = this.cr.offset();
        long theirOffset = workToCompare.cr.offset();
        return Long.compare(myOffset, theirOffset);
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
        return userFunctionSucceeded.isPresent();
    }

    public boolean isUserFunctionSucceeded() {
        Optional<Boolean> userFunctionSucceeded = this.userFunctionSucceeded;
        return userFunctionSucceeded.orElse(false);
    }

    @Override
    public String toString() {
        return StringUtils.msg("WorkContainer({}:{}:{}:{})", getTopicPartition(), cr.offset(), cr.key(), isUserFunctionSucceeded());
    }

    public boolean hasPreviouslyFailed() {
        return numberOfFailedAttempts > 0;
    }

    public Duration getTimeInFlight() {
        return Duration.ofMillis(System.currentTimeMillis()-timeTakenAsWorkMs);
    }

    public long offset() {
        return getCr().offset();
    }
}