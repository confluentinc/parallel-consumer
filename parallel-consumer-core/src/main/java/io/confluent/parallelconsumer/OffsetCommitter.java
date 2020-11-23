package io.confluent.parallelconsumer;

public interface OffsetCommitter {
    void retrieveOffsetsAndCommit();
}
