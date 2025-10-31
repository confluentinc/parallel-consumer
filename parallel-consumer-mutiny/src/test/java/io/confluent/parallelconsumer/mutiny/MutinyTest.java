package io.confluent.parallelconsumer.mutiny;

/*-
 * Copyright (C) 2020-2025 Confluent, Inc.
 */

import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class MutinyTest {

    @Test
    void emitOnExample() {
        ExecutorService executor = Executors.newFixedThreadPool(4);

        Multi<String> multi = Multi.createFrom().range(1, 3) // 1..2 inclusive
                .map(i -> 10 + i)
                .emitOn(executor) // similar to publishOn
                .map(i -> "value " + i);

        multi.subscribe().with(System.out::println, Throwable::printStackTrace);
    }

    @Test
    void runSubscriptionOnExample() {
        ExecutorService executor = Executors.newFixedThreadPool(4);

        Multi<String> multi = Multi.createFrom().range(1, 3)
                .map(i -> 10 + i)
                .runSubscriptionOn(executor) // similar to subscribeOn
                .map(i -> "value " + i);

        multi.subscribe().with(System.out::println, Throwable::printStackTrace);
    }
}
