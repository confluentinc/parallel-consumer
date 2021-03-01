package io.confluent.parallelconsumer.examples.streams;

import lombok.Value;

@Value
public class UserEvent {
    String eventPayload;
}
