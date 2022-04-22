package io.confluent.parallelconsumer.kafkabridge;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.sharedstate.CommitData;

public interface OffsetCommitter {
    void retrieveOffsetsAndCommit(CommitData offsetsToCommit);
}
