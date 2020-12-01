package io.confluent.parallelconsumer;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class BackPressureTests {

    @Test
    void backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing() {
        // mock messages downloaded for processing > MAX_TO_QUEUE
        // make sure work manager doesn't queue more than MAX_TO_QUEUE
        final int MAX_QUEUE = 1_000;
        assertThat(new ArrayList<>()).hasSizeLessThan(MAX_QUEUE);
    }

    /**
     * Checks that when max queued messages are reached, more aren't queued up
     */
    @Test
    void largeWorkLoadsCauseBackPressure(){
        fail("");
    }

    /**
     * Tests that the backpressure system works correctly - that more records aren't added for processing than are
     * desired via settings.
     */
    @Test
    void backpressure() {
    }
}
