package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ManagedTruth;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import org.junit.jupiter.api.Test;

class WorkContainerTest {

    @Test
    void basics() {
        var workContainer = new ModelUtils(new PCModuleTestEnv()).createWorkFor(0);
        ManagedTruth.assertThat(workContainer).getDelayUntilRetryDue().isNotNegative();
    }
}
