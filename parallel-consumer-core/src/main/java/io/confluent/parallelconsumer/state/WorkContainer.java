package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.WallClock;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.RecordContext;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.util.*;
import java.util.concurrent.Future;
import java.util.function.Function;

import static io.confluent.csid.utils.KafkaUtils.toTopicPartition;

@Slf4j
@EqualsAndHashCode
public class WorkContainer<K, V> implements Comparable<WorkContainer> {

    private final String DEFAULT_TYPE = "DEFAULT";

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

    private final Optional<Instant> failedAt = Optional.empty();

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

    // static instance so can't access generics - but don't need them as Options class ensures type is correct
    private static Function<Object, Duration> retryDelayProvider;

    @AllArgsConstructor
    @Value
    public static class Failure {
        Instant time;
        Throwable cause;

        public Failure(Throwable e) {
            this.time = Instant.now();
            this.cause = e;
        }
    }

    private final LinkedList<Failure> failureHistory = new LinkedList<>();

    public List<Failure> getFailureHistory() {
        return Collections.unmodifiableList(failureHistory);
    }

    public int getNumberOfFailedAttempts() {
        return failureHistory.size();
    }

    public WorkContainer(int epoch, ConsumerRecord<K, V> cr, Function<RecordContext<K, V>, Duration> retryDelayProvider) {
        this.epoch = epoch;
        this.cr = cr;
        workType = DEFAULT_TYPE;

        if (WorkContainer.retryDelayProvider == null) { // only set once
            // static instance so can't access generics - but don't need them as Options class ensures type is correct
            WorkContainer.retryDelayProvider = (Function) retryDelayProvider;
        }
    }

    public WorkContainer(int epoch, ConsumerRecord<K, V> cr, Function<RecordContext<K, V>, Duration> retryDelayProvider, String workType) {
        this(epoch, cr, retryDelayProvider);

        Objects.requireNonNull(workType);
        this.workType = workType;
    }

    public void endFlight() {
        log.trace("Ending flight {}", this);
        inFlight = false;
    }

    public boolean hasDelayPassed(WallClock clock) {
        if (!hasPreviouslyFailed()) {
            // if never failed, there is no artificial delay, so "delay" has always passed
            return true;
        }
        Duration delay = getDelayUntilRetryDue(clock);
        boolean negative = delay.isNegative() || delay.isZero(); // for debug
        return negative;
    }

    /**
     * @return time until it should be retried
     */
    public Duration getDelayUntilRetryDue(WallClock clock) {
        Instant now = clock.getNow();
        Temporal nextAttemptAt = tryAgainAt();
        return Duration.between(now, nextAttemptAt);
    }

    private Temporal tryAgainAt() {
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
        return !isInFlight();
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

    public void onUserFunctionFailure(Throwable cause) {
        log.trace("Failing {}", this);

        updateFailureHistory(cause);

        this.userFunctionSucceeded = Optional.of(false);
    }

    private void updateFailureHistory(Throwable cause) {
        Failure e = new Failure(cause);
        failureHistory.addFirst(e);
        // todo get limit from options
        if (failureHistory.size() > 10) {
            failureHistory.removeLast();
        }
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

    public boolean isAvailableToTakeAsWork(WallClock clock) {
        // todo missing boolean notAllowedMoreRecords = pm.isBlocked(topicPartition);
        return isNotInFlight() && !isUserFunctionSucceeded() && hasDelayPassed(clock);
    }

}
