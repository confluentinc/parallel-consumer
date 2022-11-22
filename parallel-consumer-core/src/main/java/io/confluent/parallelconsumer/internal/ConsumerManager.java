package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.confluent.csid.utils.StringUtils.msg;

/**
 * Delegate for {@link KafkaConsumer}
 */
@Slf4j
public class ConsumerManager<K, V> {

    /**
     * @see default.api.timeout.ms
     *         https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#consumerconfigs_default.api.timeout.ms
     */
    public static final Duration DEFAULT_API_TIMEOUT = Duration.ofSeconds(60);

    private final ParallelConsumerOptions<K, V> options;

    private final Consumer<K, V> consumer;

    private final AtomicBoolean pollingBroker = new AtomicBoolean(false);

    /**
     * Since Kakfa 2.7, multi-threaded access to consumer group metadata was blocked, so before and after polling, save
     * a copy of the metadata.
     *
     * @since 2.7.0
     */
    private ConsumerGroupMetadata metaCache;

    private int erroneousWakups = 0;
    private int correctPollWakeups = 0;
    private int noWakeups = 0;
    private boolean commitRequested;

    public ConsumerManager(ParallelConsumerOptions<K, V> options) {
        this.options = options;
        consumer = options.getConsumer();
    }

    ConsumerRecords<K, V> poll(Duration requestedLongPollTimeout) {
        Duration timeoutToUse = requestedLongPollTimeout;

        if (commitRequested) {
            log.debug("Commit requested, so will not long poll as need to perform the commit");
            timeoutToUse = Duration.ofMillis(1);// disable long poll, as commit needs performing
            commitRequested = false;
        }

        pollingBroker.set(true);
        updateMetadataCache();

        log.debug("Poll starting with timeout: {}", timeoutToUse);
        try {
            var records = consumer.poll(timeoutToUse);
            log.debug("Poll completed normally (after timeout of {}) and returned {}...", timeoutToUse, records.count());
            updateMetadataCache();
            return records;
        } catch (WakeupException w) {
            correctPollWakeups++;
            log.debug("Awoken from broker poll, returning early with empty poll set");
            log.trace("Wakeup caller is:", w);
            return new ConsumerRecords<>(UniMaps.of());
        } finally {
            pollingBroker.set(false);
        }
    }

    protected void updateMetadataCache() {
        metaCache = consumer.groupMetadata();
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

    /**
     * Any failures are bubbled up to the caller
     */
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend) throws PCTimeoutException, PCCommitFailedException {
        // we don't want to be woken up during a commit, only polls
        boolean inProgress = true;
        noWakeups++;
        while (inProgress) {
            Duration timeout = DEFAULT_API_TIMEOUT;
            try {
                log.debug("Committing offsets Sync with timeout: {}", timeout);
                consumer.commitSync(offsetsToSend, timeout);
                inProgress = false;
            } catch (CommitFailedException e) {
                throw new PCCommitFailedException(e);
            } catch (org.apache.kafka.common.errors.TimeoutException e) { // distinguish from java.util.TimeoutException
                throw new PCTimeoutException("Timeout committing offsets from KafkaConsumer", e);
            } catch (WakeupException w) {
                // todo remove after actor system merged
                log.debug(msg("Got woken up, retry. errors: {} none: {} correct: {}", erroneousWakups, noWakeups, correctPollWakeups), w);
                erroneousWakups++;
            }
        }
    }

    /**
     * Fire and forget
     */
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        OffsetCommitCallback offsetCommitCallback = (failedOffsets, exception) -> {
            if (exception != null) {
                log.error(msg("Error committing offsets in async mode. Offset: {}", failedOffsets), exception);
            }
        };
        consumer.commitAsync(offsets, offsetCommitCallback);
    }

    public ConsumerGroupMetadata groupMetadata() {
        return metaCache;
    }

    public void close(final Duration defaultTimeout) {
        consumer.close(defaultTimeout);
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

    public void resume(final Set<TopicPartition> pausedTopics) {
        consumer.resume(pausedTopics);
    }

    public void onCommitRequested() {
        this.commitRequested = true;
    }
}
