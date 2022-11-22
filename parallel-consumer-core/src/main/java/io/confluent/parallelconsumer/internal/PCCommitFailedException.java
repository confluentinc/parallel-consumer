package io.confluent.parallelconsumer.internal;

import lombok.experimental.StandardException;

/**
 * @author Antony Stubbs
 * @see org.apache.kafka.clients.consumer.CommitFailedException
 */
@StandardException
public class PCCommitFailedException extends InternalException {
}
