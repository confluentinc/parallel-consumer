package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.experimental.StandardException;

/**
 * Thrown when magic number for Kafka Streams offset metadata V1 is found.
 *
 * @author Nacho Munoz
 */
@StandardException
public class KafkaStreamsV1EncodingNotSupported extends EncodingNotSupportedException{
}
