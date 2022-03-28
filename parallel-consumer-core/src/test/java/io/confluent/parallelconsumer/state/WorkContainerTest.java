package io.confluent.parallelconsumer.state;

import io.confluent.parallelconsumer.ManagedTruth;
import org.junit.jupiter.api.Test;

class WorkContainerTest {

    @Test
    void basics() {
        var workContainer = ModelUtils.createWorkFor(0);
        ManagedTruth.assertThat(workContainer).getDelayUntilRetryDue().isNotNegative();
    }
}
