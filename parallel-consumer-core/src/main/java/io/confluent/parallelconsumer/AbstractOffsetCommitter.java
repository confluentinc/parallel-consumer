package io.confluent.parallelconsumer;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
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
    @SneakyThrows
    @Override
    public void retrieveOffsetsAndCommit() {
        log.debug("Commit process starting");
        consumerMgr.aquireLock();

        try {
            // todo shouldn't be removed until commit succeeds (there's no harm in committing the same offset twice)
            preAcquireWork();
            try {
                //Map<TopicPartition, OffsetAndMetadata> offsetsToSend = wm.findCompletedEligibleOffsetsAndRemove();
                Map<TopicPartition, OffsetAndMetadata> offsetsToSend = wm.serialiseEncoders();
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
        }finally {
            consumerMgr.releaseLock();
        }
    }

    protected void postCommit() {
        // default noop
    }

    protected void preAcquireWork() {
        // default noop
    }

    private void onOffsetCommitSuccess(final Map<TopicPartition, OffsetAndMetadata> offsetsCommitted) {
        wm.onOffsetCommitSuccess(offsetsCommitted);
    }

    protected abstract void commitOffsets(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend, final ConsumerGroupMetadata groupMetadata);

}
