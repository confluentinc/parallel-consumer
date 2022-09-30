package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.RecordContext;
import io.confluent.parallelconsumer.internal.ProducerManager;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Function;

import static io.confluent.csid.utils.KafkaUtils.toTopicPartition;
import static java.util.Optional.of;

@Slf4j
@EqualsAndHashCode
public class WorkContainer<K, V> implements Comparable<WorkContainer<K, V>> {

    static final String DEFAULT_TYPE = "DEFAULT";

    /**
     * Assignment generation this record comes from. Used for fencing messages after partition loss, for work lingering
     * in the system of in flight.
     */
    @Getter
    private final long epoch;

    /**
     * Simple way to differentiate treatment based on type
     */
    @Getter
    @Setter
    // todo change to enum, remove setter - #241
    private String workType;

    @Getter
    private final ConsumerRecord<K, V> cr;

    private final Clock clock;

    @Getter
    private int numberOfFailedAttempts = 0;

    @Getter
    private Optional<Instant> lastFailedAt = Optional.empty();

    @Getter
    private Optional<Instant> succeededAt = Optional.empty();

    @Getter
    private Optional<Throwable> lastFailureReason;

    private boolean inFlight = false;

    @Getter
    private Optional<Boolean> maybeUserFunctionSucceeded = Optional.empty();

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

    public WorkContainer(long epoch, ConsumerRecord<K, V> cr, Function<RecordContext<K, V>, Duration> retryDelayProvider, String workType, Clock clock) {
        Objects.requireNonNull(workType);

        this.epoch = epoch;
        this.cr = cr;
        this.workType = workType;
        this.clock = clock;

        if (WorkContainer.retryDelayProvider == null) { // only set once
            // static instance so can't access generics - but don't need them as Options class ensures type is correct
            WorkContainer.retryDelayProvider = (Function) retryDelayProvider;
        }
    }

    public WorkContainer(long epoch, ConsumerRecord<K, V> cr, Function<RecordContext<K, V>, Duration> retryDelayProvider, Clock clock) {
        this(epoch, cr, retryDelayProvider, DEFAULT_TYPE, clock);
    }

    public void endFlight() {
        log.trace("Ending flight {}", this);
        inFlight = false;
    }

    public boolean hasDelayPassed() {
        if (!hasPreviouslyFailed()) {
            // if never failed, there is no artificial delay, so "delay" has always passed
            return true;
        }
        Duration delay = getDelayUntilRetryDue();
        boolean negative = delay.isNegative() || delay.isZero(); // for debug
        return negative;
    }

    /**
     * @return time until it should be retried
     */
    public Duration getDelayUntilRetryDue() {
        Instant now = clock.instant();
        Temporal nextAttemptAt = getRetryDueAt();
        return Duration.between(now, nextAttemptAt);
    }

    /**
     * @return The point in time at which the record should ideally be retried.
     */
    public Instant getRetryDueAt() {
        if (lastFailedAt.isPresent()) {
            // previously failed, so add the delay to the last failed time
            Duration retryDelay = getRetryDelayConfig();
            return lastFailedAt.get().plus(retryDelay);
        } else {
            // never failed, so no try again delay
            return Instant.MIN; // use a constant for stable comparison
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
        timeTakenAsWorkMs = of(System.currentTimeMillis());
    }

    public TopicPartition getTopicPartition() {
        return toTopicPartition(getCr());
    }

    public void onUserFunctionSuccess() {
        this.succeededAt = of(clock.instant());
        this.maybeUserFunctionSucceeded = of(true);
    }

    public void onUserFunctionFailure(Throwable cause) {
        log.trace("Failing {}", this);

        updateFailureHistory(cause);

        this.maybeUserFunctionSucceeded = of(false);
    }

    private void updateFailureHistory(Throwable cause) {
        numberOfFailedAttempts++;
        lastFailedAt = of(Instant.now(clock));
        lastFailureReason = Optional.ofNullable(cause);
    }

    public boolean isUserFunctionComplete() {
        return this.getMaybeUserFunctionSucceeded().isPresent();
    }

    public boolean isUserFunctionSucceeded() {
        Optional<Boolean> userFunctionSucceeded = this.getMaybeUserFunctionSucceeded();
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

    /**
     * Checks the work is not already in flight, it's retry delay has passed and that it's not already been succeeded.
     * <p>
     * Checking that there's no back pressure for the partition it belongs to is covered by
     * {@link PartitionStateManager#isAllowedMoreRecords(WorkContainer)}.
     */
    public boolean isAvailableToTakeAsWork() {
        return isNotInFlight() && !isUserFunctionSucceeded() && hasDelayPassed();
    }

    /**
     * Only unlock our producing lock, when we've had the {@link WorkContainer} state safely returned to the controllers
     * inbound queue, so we know it'll be included properly before the next commit as a succeeded offset. As in order
     * for the controller to perform the transaction commit, it will be blocked from acquiring its commit lock until all
     * produce locks have been returned, inbound queue processed, and thus their representative offsets placed into the
     * commit payload (offset map).
     */
    public void onPostAddToMailBox(PollContextInternal<K, V> context, Optional<ProducerManager<K, V>> producerManager) {
        producerManager.ifPresent(pm -> {
            var producingLock = context.getProducingLock();
            producingLock.ifPresent(pm::finishProducing);
        });
    }
}
