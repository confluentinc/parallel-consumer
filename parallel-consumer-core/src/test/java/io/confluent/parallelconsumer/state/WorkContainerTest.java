package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ManagedTruth;
import org.junit.jupiter.api.Test;

class WorkContainerTest {

    @Test
    void basics() {
        var workContainer = ModelUtils.createWorkFor(0);
        ManagedTruth.assertThat(workContainer).getDelayUntilRetryDue().isNotNegative();
    }
}
