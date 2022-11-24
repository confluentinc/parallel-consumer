package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.experimental.StandardException;

/**
 * Thrown when magic number for Kafka Streams offset metadata is found.
 * @see <a href="https://github.com/apache/kafka/blob/cc77a38d280657a0e3969b255f103af4d11c7914/streams/src/main/java/org/apache/kafka/streams/processor/internals/TopicPartitionMetadata.java#L33">Kafka Streams magic number</a>
 * @author Nacho Munoz
 */
@StandardException
public class KafkaStreamsEncodingNotSupported extends EncodingNotSupportedException{
}
