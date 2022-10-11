package io.confluent.parallelconsumer.integrationTests.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
class PartitionStateCommittedOffsetIT extends BrokerIntegrationTest<String, String> {

    AdminClient ac;

    String groupId = getKcu().getConsumer().groupMetadata().groupId();

    TopicPartition tp = new TopicPartition("topic", 0);

    /**
     * Test for offset gaps in partition data (i.e. compacted topics)
     */
    void compactedTopic() {

    }

    /**
     * CG offset has been changed to a lower offset (partition rewind / replay) (metdata lost?)
     */
    void committedOffsetLower() {
//        ac.alterConsumerGroupOffsets(groupId, )
    }

    /**
     * CG offset has been changed to something higher than expected (offset skip) (metdata lost?)
     */
    void committedOffsetHigher() {
    }

    /**
     * CG offset has disappeared - committed offset hasn't been changed, but broker gives us a bootstrap poll result
     * with a higher offset than expected. Could be caused by retention period, or compaction.
     */
    void committedOffsetRemoved() {
    }

    void cgOffsetsDeleted() {
//        ac.deleteConsumerGroupOffsets()

    }


}