package io.confluent.parallelconsumer.examples.streams;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */


import lombok.Value;

@Value
public class UserEvent {
    String eventPayload;
}
