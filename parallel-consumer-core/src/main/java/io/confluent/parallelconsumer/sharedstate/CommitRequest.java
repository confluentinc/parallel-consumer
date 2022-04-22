package io.confluent.parallelconsumer.sharedstate;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.Value;

import java.util.UUID;

/**
 * Commit request message
 */
@Value
public class CommitRequest {
    UUID id = UUID.randomUUID();
    long requestedAtMs = System.currentTimeMillis();
    CommitData commitData;
}
