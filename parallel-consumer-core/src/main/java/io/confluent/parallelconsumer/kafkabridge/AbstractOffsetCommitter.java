package io.confluent.parallelconsumer.kafkabridge;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.controller.WorkManager;
import io.confluent.parallelconsumer.sharedstate.CommitData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractOffsetCommitter<K, V> implements OffsetCommitter {

    protected final ConsumerManager<K, V> consumerMgr;
    protected final WorkManager<K, V> wm;

    /**
     * Get offsets from {@link WorkManager} that are ready to commit
     */
    @Override
    public void retrieveOffsetsAndCommit(CommitData offsetsToCommit) {
        log.debug("Commit starting - find completed work to commit offsets");
        preAcquireWork();
        try {
            if (offsetsToCommit.isEmpty()) {
                log.debug("No offsets ready");
            } else {
                log.debug("Will commit offsets for {} partition(s): {}", offsetsToCommit.size(), offsetsToCommit);
                ConsumerGroupMetadata groupMetadata = consumerMgr.groupMetadata();

                log.debug("Begin commit");
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

    protected void preAcquireWork() {
        // default noop
    }

    private void onOffsetCommitSuccess(CommitData committed) {
        wm.onOffsetCommitSuccess(committed);
    }

    protected abstract void commitOffsets(final CommitData offsetsToSend, final ConsumerGroupMetadata groupMetadata);

}
