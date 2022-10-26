package io.confluent.parallelconsumer.sanity;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedDeque;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sanity test usage
 */
public class QueueingSanityTests {

    @Test
    public void test() {
        ConcurrentLinkedDeque<Integer> q = new ConcurrentLinkedDeque<>();

        assertThat(q.add(1)).isTrue();
        assertThat(q.add(2)).isTrue();

        assertThat(q.poll()).isEqualTo(1);
        assertThat(q.poll()).isEqualTo(2);
    }
}
