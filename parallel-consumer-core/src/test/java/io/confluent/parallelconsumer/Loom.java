package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class Loom {
    @Test
    void loom() {
        Thread.ofVirtual();
        Thread thread = Thread.startVirtualThread(() -> {
            log.info("hi!");
        });
        thread.start();
        thread.join();
        ManagedTruth.assertThat(thread)
    }
}
