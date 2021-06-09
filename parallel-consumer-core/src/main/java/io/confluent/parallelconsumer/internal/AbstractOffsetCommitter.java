package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.WorkManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractOffsetCommitter<K, V> implements OffsetCommitter {

    protected final ConsumerManager<K, V> consumerMgr;
    protected final WorkManager<K, V> wm;

    /**
     * Get offsets from {@link WorkManager} that are ready to commit
     */
    @Override
    public void retrieveOffsetsAndCommit() {
        log.debug("Commit starting - find completed work to commit offsets");
        // todo shouldn't be removed until commit succeeds (there's no harm in committing the same offset twice)
        preAcquireWork();
        try {
            Map<TopicPartition, OffsetAndMetadata> offsetsToSend = wm.findCompletedEligibleOffsetsAndRemove();
            if (offsetsToSend.isEmpty()) {
                log.trace("No offsets ready");
            } else {
                log.debug("Will commit offsets for {} partition(s): {}", offsetsToSend.size(), offsetsToSend);
                ConsumerGroupMetadata groupMetadata = consumerMgr.groupMetadata();

                log.debug("Begin commit");
                commitOffsets(offsetsToSend, groupMetadata);

                log.debug("On commit success");
                onOffsetCommitSuccess(offsetsToSend);
            }
        } finally {
            postCommit();
        }
    }

    protected void postCommit() {
        // default noop
    }

    protected void preAcquireWork() {
        // default noop
    }

    private void onOffsetCommitSuccess(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend) {
        wm.onOffsetCommitSuccess(offsetsToSend);
    }

    protected abstract void commitOffsets(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend, final ConsumerGroupMetadata groupMetadata);

}
