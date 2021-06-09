package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

public interface OffsetCommitter {
    void retrieveOffsetsAndCommit();
}
