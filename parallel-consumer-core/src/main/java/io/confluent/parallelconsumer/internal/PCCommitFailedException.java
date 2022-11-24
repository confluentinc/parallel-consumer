package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.experimental.StandardException;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Checked exception version of {@link CommitFailedException}.
 * <p>
 * Copied from {@link CommitFailedException}: This exception is raised when an offset commit with
 * {@link KafkaConsumer#commitSync()} fails with an unrecoverable error. This can happen when a group rebalance
 * completes before the commit could be successfully applied. In this case, the commit cannot generally be retried
 * because some partitions may have already been assigned to another member in the group.
 *
 * @author Antony Stubbs
 * @see CommitFailedException
 */
@StandardException
public class PCCommitFailedException extends InternalException {
}
