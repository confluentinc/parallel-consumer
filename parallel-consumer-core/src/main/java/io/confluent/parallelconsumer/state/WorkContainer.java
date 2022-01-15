package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.WallClock;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
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

import static io.confluent.csid.utils.KafkaUtils.toTopicPartition;

@Slf4j
@EqualsAndHashCode
public class WorkContainer<K, V> implements Comparable<WorkContainer> {

    private final String DEFAULT_TYPE = "DEFAULT";

    final static WallClock normalClock = new WallClock();

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

    @Getter
    private Optional<Boolean> userFunctionSucceeded = Optional.empty();

    /**
     * @see ParallelConsumerOptions#getDefaultMessageRetryDelay()
     */
    @Setter
    static Duration defaultRetryDelay = Duration.ofSeconds(1);

    @Getter
    @Setter(AccessLevel.PUBLIC)
    private Future<List<?>> future;

    private Optional<Long> timeTakenAsWorkMs = Optional.empty();

    public WorkContainer(int epoch, ConsumerRecord<K, V> cr) {
        this.epoch = epoch;
        this.cr = cr;
        workType = DEFAULT_TYPE;
    }

    public WorkContainer(int epoch, ConsumerRecord<K, V> cr, String workType) {
        this.epoch = epoch;
        this.cr = cr;
        Objects.requireNonNull(workType);
        this.workType = workType;
    }

    public void fail(WallClock clock) {
        // If not explicitly retriable, put it back in with a retry counter, so it can be later given up on
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
        if (!hasPreviouslyFailed()) {
            // if never failed, there is no artificial delay, so "delay" has always passed
            return true;
        }
        Duration delay = getDelayUntilRetryDue(clock);
        boolean negative = delay.isNegative() || delay.isZero();
        return negative;
    }

    /**
     * @return time until it should be retried
     */
    public Duration getDelayUntilRetryDue() {
        return getDelayUntilRetryDue(normalClock);
    }

    /**
     * @return time until it should be retried
     */
    public Duration getDelayUntilRetryDue(WallClock clock) {
        Instant now = clock.getNow();
        Temporal nextAttemptAt = tryAgainAt(clock);
        return Duration.between(now, nextAttemptAt);
    }

    private Temporal tryAgainAt(WallClock clock) {
        if (failedAt.isPresent()) {
            // previously failed, so add the delay to the last failed time
            Duration retryDelay = getRetryDelayConfig();
            return failedAt.get().plus(retryDelay);
        } else {
            // never failed, so no try again delay
            return Instant.now();
        }
    }

    /**
     * @return the delay between retries e.g. retry after 1 second
     */
    public Duration getRetryDelayConfig() {
        var retryDelayProvider = ParallelConsumerOptions.retryDelayProviderStatic;
        if (retryDelayProvider != null) {
            return retryDelayProvider.apply(this);
        } else {
            return defaultRetryDelay;
        }
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

    public void onQueueingForExecution() {
        log.trace("Queueing for execution: {}", this);
        inFlight = true;
        timeTakenAsWorkMs = Optional.of(System.currentTimeMillis());
    }

    public TopicPartition getTopicPartition() {
        return toTopicPartition(getCr());
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
        return "WorkContainer(" + toTopicPartition(cr) + ":" + cr.offset() + ":" + cr.key() + ")";
    }

    public Duration getTimeInFlight() {
        if (!timeTakenAsWorkMs.isPresent()) {
            return Duration.ZERO;
        }
        long millis = System.currentTimeMillis() - timeTakenAsWorkMs.get();
        return Duration.ofMillis(millis);
    }

    public long offset() {
        return getCr().offset();
    }

    public boolean hasPreviouslyFailed() {
        return getNumberOfFailedAttempts() > 0;
    }

    public boolean isAvailableToTakeAsWork() {
        // todo missing boolean notAllowedMoreRecords = pm.isBlocked(topicPartition);
        return isNotInFlight() && !isUserFunctionSucceeded() && hasDelayPassed(normalClock);
    }
}
