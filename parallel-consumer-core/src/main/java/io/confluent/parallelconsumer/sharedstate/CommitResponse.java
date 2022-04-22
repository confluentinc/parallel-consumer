package io.confluent.parallelconsumer.sharedstate;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.Value;

/**
 * Commit response message, linked to a {@link CommitRequest}
 */
@Value
public class CommitResponse {
    CommitRequest request;
}
