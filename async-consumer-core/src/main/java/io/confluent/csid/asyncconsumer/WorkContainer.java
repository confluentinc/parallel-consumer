package io.confluent.csid.asyncconsumer;

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
    // TODO remove in favour of class sub-typing
    private String workType;

    @Getter
    private final ConsumerRecord<K, V> cr;
    private int numberOfAttempts;
    private Optional<Instant> failedAt = Optional.empty();
    private boolean inFlight = false;
    @Getter
    private Optional<Boolean> userFunctionSucceeded = Optional.empty();

    @Getter
    private static Duration retryDelay = Duration.ofSeconds(10);

    @Getter
    @Setter(AccessLevel.PACKAGE)
    private Future<List<Object>> future;

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
        numberOfAttempts++;
        failedAt = Optional.of(clock.getNow());
        inFlight = false;
    }

    public void succeed() {
        log.trace("Succeeded {}", this);
        inFlight = false;
    } // todo ??

    public boolean hasDelayPassed(WallClock clock) {
        long delay = getDelay(TimeUnit.SECONDS, clock);
        boolean delayHasPassed = delay <= 0;
        return delayHasPassed;
    }

    //    @Override
    public long getDelay(TimeUnit unit, WallClock clock) {
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
    public int compareTo(WorkContainer o) {
        long myOffset = this.cr.offset();
        long theirOffset = o.cr.offset();
        int compare = Long.compare(myOffset, theirOffset);
        return compare;
    }

    public boolean isNotInFlight() {
        return !inFlight;
    }

    public void takingAsWork() {
        log.trace("Being taken as work: {}", this);
        inFlight = true;
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

    public boolean isComplete() {
        return this.getUserFunctionSucceeded().isPresent();
    }

    @Override
    public String toString() {
        return "WorkContainer(" + toTP(cr) + ":" + cr.offset() + ":" + cr.key() + ":" + cr.value() + ")";
    }
}