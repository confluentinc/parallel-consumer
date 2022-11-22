package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.WorkManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.TimeoutException;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractOffsetCommitter<K, V> implements OffsetCommitter {

    protected final ConsumerManager<K, V> consumerMgr;

    protected final WorkManager<K, V> wm;

    /**
     * Get offsets from {@link WorkManager} that are ready to commit
     */
    @Override
    public void retrieveOffsetsAndCommit() throws
            PCTimeoutException,
            InterruptedException,
            TimeoutException, // java.util.concurrent.TimeoutException
            PCCommitFailedException
    {
        log.debug("Find completed work to commit offsets");
        preAcquireOffsetsToCommit();
        try {
            var offsetsToCommit = wm.collectCommitDataForDirtyPartitions();
            if (offsetsToCommit.isEmpty()) {
                log.debug("No offsets ready");
            } else {
                log.debug("Will commit offsets for {} partition(s): {}", offsetsToCommit.size(), offsetsToCommit);
                ConsumerGroupMetadata groupMetadata = consumerMgr.groupMetadata();

                log.debug("Begin commit offsets");
                commitOffsets(offsetsToCommit, groupMetadata);

                log.debug("On commit success");
                onOffsetCommitSuccess(offsetsToCommit);
            }
        } finally {
            postCommit();
        }
    }

    protected void postCommit() {
        // default noop
    }

    protected void preAcquireOffsetsToCommit() throws TimeoutException, InterruptedException {
        // default noop
    }

    private void onOffsetCommitSuccess(final Map<TopicPartition, OffsetAndMetadata> committed) {
        wm.onOffsetCommitSuccess(committed);
    }

    protected abstract void commitOffsets(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend, final ConsumerGroupMetadata groupMetadata) throws PCTimeoutException, PCCommitFailedException;

}
