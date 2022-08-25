package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public interface OffsetCommitter {
    void retrieveOffsetsAndCommit();
}
