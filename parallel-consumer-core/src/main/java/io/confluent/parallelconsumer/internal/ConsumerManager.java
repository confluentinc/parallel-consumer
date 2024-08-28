package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Delegate for {@link KafkaConsumer}
 */
@Slf4j
@RequiredArgsConstructor
public class ConsumerManager<K, V> {

    private final Consumer<K, V> consumer;

    private final Duration offsetCommitTimeout;

    private final Duration saslAuthenticationRetryTimeout;

    private final Duration saslAuthenticationRetryBackOff;

    private final AtomicBoolean pollingBroker = new AtomicBoolean(false);

    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    private final AtomicLong pendingRequests = new AtomicLong(0L);
    /**
     * Since Kakfa 2.7, multi-threaded access to consumer group metadata was blocked, so before and after polling, save
     * a copy of the metadata.
     *
     * @since 2.7.0
     */
    private ConsumerGroupMetadata metaCache;

    private volatile int pausedPartitionSizeCache = 0;

    private int erroneousWakups = 0;
    private int correctPollWakeups = 0;
    private int noWakeups = 0;
    private boolean commitRequested;

    ConsumerRecords<K, V> poll(Duration requestedLongPollTimeout) {
        Duration timeoutToUse = requestedLongPollTimeout;
        ConsumerRecords<K, V> records = null;
        try {
            if (commitRequested) {
                log.debug("Commit requested, so will not long poll as need to perform the commit");
                timeoutToUse = Duration.ofMillis(1);// disable long poll, as commit needs performing
                commitRequested = false;
            }
            pollingBroker.set(true);
            updateCache();
            log.debug("Poll starting with timeout: {}", timeoutToUse);
            Instant pollStarted = Instant.now();
            long tryCount = 0;
            boolean polledSuccessfully = false;
            try {
                pendingRequests.addAndGet(1L);
                while (!shutdownRequested.get()) {
                    tryCount++;
                    try {
                        records = consumer.poll(timeoutToUse);
                        polledSuccessfully = true;
                        break;
                    } catch (SaslAuthenticationException authenticationException) {
                        Instant now = Instant.now();
                        Duration elapsed = Duration.between(pollStarted, now);
                        boolean shouldRetry = elapsed.toMillis() < saslAuthenticationRetryTimeout.toMillis();
                        if (shouldRetry) {
                            log.warn("Poll error: SaslAuthenticationException. Retrying ({})", tryCount);
                            try {
                                retryBackOff(this.saslAuthenticationRetryBackOff.toMillis()); // no need to check return value here as next loop will check
                            } catch (InterruptedException ex) {
                                throw new RuntimeException("Poll interrupted", ex);
                            }
                        } else {
                            // no more retries allowed
                            log.error("Poll error: SaslAuthenticationException. {} tries attempted, since {}", tryCount, pollStarted, authenticationException);
                            throw authenticationException;
                        }
                    }
                }
            } finally {
                if (polledSuccessfully) {
                    log.debug("Poll completed normally (after timeout of {} on try {}) and returned {}...", timeoutToUse, tryCount, records.count());
                } else {
                    log.debug("Poll did not completed (after timeout of {} and tries {}), shutdownRequested {}", timeoutToUse, tryCount, shutdownRequested.get());
                }
                pendingRequests.addAndGet(-1L);
            }
            updateCache();
        } catch (WakeupException w) {
            correctPollWakeups++;
            log.debug("Awoken from broker poll");
            log.trace("Wakeup caller is:", w);
            records = new ConsumerRecords<>(UniMaps.of());
        } finally {
            pollingBroker.set(false);
        }
        return records != null ? records : new ConsumerRecords<>(UniMaps.of());
    }

    protected void updateCache() {
        metaCache = consumer.groupMetadata();
        pausedPartitionSizeCache = consumer.paused().size();
    }

    /**
     * Wakes up the consumer, but only if it's polling.
     * <p>
     * Otherwise, we can interrupt other operations like {@link KafkaConsumer#commitSync()}.
     */
    public void wakeup() {
        // boolean reduces the chances of a mis-timed call to wakeup, but doesn't prevent all spurious wake up calls to other methods like #commit
        // if the call to wakeup happens /after/ the check for a wake up state inside #poll, then the next call will through the wake up exception (i.e. #commit)
        if (pollingBroker.get()) {
            log.debug("Waking up consumer");
            consumer.wakeup();
        }
    }

    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend) {
        // we don't want to be woken up during a commit, only polls
        boolean inProgress = true;
        noWakeups++;
        while (inProgress) {
            try {
                pendingRequests.addAndGet(1L);
                long tryCount = 0;
                //allow to try to commit at least once during close / shutdown regardless of the flag.
                while (tryCount == 0 || !shutdownRequested.get()) {
                    tryCount++;
                    Instant startedTime = Instant.now();
                    try {
                        consumer.commitSync(offsetsToSend);
                        // break when offset commit is okay. Do not throw exception to main threads
                        break;
                    } catch(CommitFailedException commitFailedException) {
                        // it is impossible to commit now because the group have rebalanced
                        // Log an error and let the poller do the rebalance job and seek commit later
                        log.warn("Failed to commit offset due to group rebalancing. Will ignore the error for now.", commitFailedException);
                        break;
                    } catch(TimeoutException timeoutException) {
                        // offset commit times out after 1 minute.
                        // We should honor the user configured timeout offsetCommitTimeout here.
                        Instant now = Instant.now();
                        Duration elapsed = Duration.between(startedTime, now);
                        boolean shouldRetry = elapsed.toMillis() <= offsetCommitTimeout.toMillis();
                        if(shouldRetry) {
                            log.warn("Encountered timeout while committing offset. Retrying ({})", tryCount);
                            // The timeout is already after 1 minute. There is no need to sleep in between retries
                        } else {
                            // bubble up other exceptions for main events to handle
                            log.error("Offset commit took too long due to TimeoutException (tried {} times)", tryCount);
                            throw timeoutException;
                        }
                    } catch(SaslAuthenticationException authenticationException) {
                        // We should honor the user configured SaslAuthenticationException timeout here.
                        // to allow the program to sustain temporary LDAP failures
                        Instant now = Instant.now();
                        Duration elapsed = Duration.between(startedTime, now);
                        boolean shouldRetry = elapsed.toMillis() <= saslAuthenticationRetryTimeout.toMillis();
                        if(shouldRetry) {
                            log.warn("Encountered SaslAuthenticationException while committing offset. Retrying ({})", tryCount);
                            // Since authentication exception may happen immediately, it is good to sleep a few seconds before trying again
                            try {
                                retryBackOff(saslAuthenticationRetryBackOff.toMillis()); // no need to check return value
                            } catch(InterruptedException ex) {
                                // don't swallow the interrupted exception
                                log.warn("Offset Commit was interrupted", ex);
                                throw new RuntimeException("Offset Commit was interrupted");
                            }
                        } else {
                            log.error("Offset commit failed due to SaslAuthenticationException (tried {} times)", tryCount);
                            // bubble up other exceptions for main events to handle
                            throw authenticationException;
                        }
                    }
                }
                inProgress = false;
            } catch (WakeupException w) {
                log.debug("Got woken up, retry. errors: " + erroneousWakups + " none: " + noWakeups + " correct:" + correctPollWakeups, w);
                erroneousWakups++;
            } finally {
                pendingRequests.addAndGet(-1L);
            }
        }
    }

    // Return true if backoff is finished successfully
    // Return false if it ended before the timeout
    // Throws InterruptedException if interrupted
    private boolean retryBackOff(long backOffTimeMs) throws InterruptedException {
        int interval = 100; // sleep in 100ms interval
        long started = System.currentTimeMillis();
        long deadLine = started + backOffTimeMs;
        while(System.currentTimeMillis() < deadLine) {
            Thread.sleep(interval);
            if(shutdownRequested.get()) {
                return false;
            }
        }
        return true;
    }
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        // we dont' want to be woken up during a commit, only polls
        boolean inProgress = true;
        noWakeups++;
        while (inProgress) {
            try {
                consumer.commitAsync(offsets, callback);
                inProgress = false;
            } catch (WakeupException w) {
                log.debug("Got woken up, retry. errors: " + erroneousWakups + " none: " + noWakeups + " correct:" + correctPollWakeups, w);
                erroneousWakups++;
            }
        }
    }

    public ConsumerGroupMetadata groupMetadata() {
        return metaCache;
    }

    public void signalStop() {
        if(!this.shutdownRequested.get()) {
            log.info("Signaling Consumer Manager to stop");
            this.shutdownRequested.set(true);
        }
    }

    public void close(final Duration defaultTimeout) {
        long deadline = System.currentTimeMillis() + defaultTimeout.toMillis();
        log.debug("Consumer Manager Closing...");
        this.shutdownRequested.set(true);
        log.debug("ConsumerManager close waiting for max of {} for pending requests to complete", defaultTimeout);
        while(pendingRequests.get() > 0L && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(100);
            } catch(InterruptedException ex) {
                throw new RuntimeException("Wait interrupted");
            }
        }
        log.debug("ConsumerManager close wait completed.");
        consumer.close(defaultTimeout);
        log.debug("ConsumerManager closed");
    }

    public Set<TopicPartition> assignment() {
        return consumer.assignment();
    }

    public void pause(final Set<TopicPartition> assignment) {
        consumer.pause(assignment);
    }

    public Set<TopicPartition> paused() {
        return consumer.paused();
    }

    public int getPausedPartitionSize() {
        return pausedPartitionSizeCache;
    }

    public void resume(final Set<TopicPartition> pausedTopics) {
        consumer.resume(pausedTopics);
    }

    public void onCommitRequested() {
        this.commitRequested = true;
    }
}
