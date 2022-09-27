package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.RecordContext;
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

import static io.confluent.csid.utils.KafkaUtils.toTopicPartition;
import static java.util.Optional.of;

@Slf4j
@EqualsAndHashCode
public class WorkContainer<K, V> implements Comparable<WorkContainer<K, V>> {

    static final String DEFAULT_TYPE = "DEFAULT";

    /**
     * Reference to parent for memory efficient static object access with generic parameters.
     * <p>
     * Not static, but only a single reference - replacing previous single reference, but allows for access to several
     * global state instances and simplifies the architecture.
     *
     * @see PartitionStateManager#getClock
     * @see PartitionStateManager#getOptions
     */
    @NonNull
    private final PartitionStateManager<K, V> partitionStateManagerParent;

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

    @Getter
    @Setter(AccessLevel.PUBLIC)
    private Future<List<?>> future;

    private Optional<Long> timeTakenAsWorkMs = Optional.empty();


    public WorkContainer(long epoch, ConsumerRecord<K, V> cr, String workType, PartitionStateManager<K, V> psm) {
        Objects.requireNonNull(workType);

        this.epoch = epoch;
        this.cr = cr;
        this.workType = workType;

        this.partitionStateManagerParent = psm;
    }

    public WorkContainer(long epoch, ConsumerRecord<K, V> cr, PartitionStateManager<K, V> psm) {
        this(epoch, cr, DEFAULT_TYPE, psm);
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
        Instant now = partitionStateManagerParent.getClock().instant();
        Temporal nextAttemptAt = tryAgainAt();
        return Duration.between(now, nextAttemptAt);
    }

    private Temporal tryAgainAt() {
        if (lastFailedAt.isPresent()) {
            // previously failed, so add the delay to the last failed time
            Duration retryDelay = getRetryDelayConfig();
            return lastFailedAt.get().plus(retryDelay);
        } else {
            // never failed, so no try again delay
            return Instant.now();
        }
    }

    /**
     * @return the delay between retries e.g. retry after 1 second
     */
    public Duration getRetryDelayConfig() {
        var options = partitionStateManagerParent.getOptions();
        var retryDelayProvider = options.getRetryDelayProvider();
        if (retryDelayProvider != null) {
            return retryDelayProvider.apply(new RecordContext<>(this));
        } else {
            return options.getDefaultMessageRetryDelay();
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
        this.succeededAt = of(partitionStateManagerParent.getClock().instant());
        this.maybeUserFunctionSucceeded = of(true);
    }

    public void onUserFunctionFailure(Throwable cause) {
        log.trace("Failing {}", this);

        updateFailureHistory(cause);

        this.maybeUserFunctionSucceeded = of(false);
    }

    private void updateFailureHistory(Throwable cause) {
        numberOfFailedAttempts++;
        lastFailedAt = of(Instant.now(partitionStateManagerParent.getClock()));
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

    public boolean isAvailableToTakeAsWork() {
        // todo missing boolean notAllowedMoreRecords = pm.isBlocked(topicPartition);
        return isNotInFlight() && !isUserFunctionSucceeded() && hasDelayPassed();
    }

}
