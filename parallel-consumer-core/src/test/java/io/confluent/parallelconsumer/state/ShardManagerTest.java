package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.TimeUtils;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import org.junit.jupiter.api.Test;

import java.util.NavigableSet;

import static com.google.common.truth.Truth.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * @author Antony Stubbs
 * @see ShardManager
 */
class ShardManagerTest {

    ModelUtils mu = new ModelUtils();

    @Test
    void retryQueueOrdering() {
        PCModuleTestEnv module = mu.getModule();
        ShardManager<String, String> sm = new ShardManager<>(module.options(), module.workManager(), TimeUtils.getClock());
        NavigableSet<WorkContainer<?, ?>> retryQueue = sm.getRetryQueue();


        WorkContainer<String, String> w0 = mu.createWorkFor(0);
        WorkContainer<String, String> w1 = mu.createWorkFor(1);
        WorkContainer<String, String> w2 = mu.createWorkFor(2);
        WorkContainer<String, String> w3 = mu.createWorkFor(3);

        final int ZERO = 0;
        assertThat(sm.getRetryQueueWorkContainerComparator().compare(w0, w0)).isEqualTo(ZERO);


        retryQueue.add(w0);
        retryQueue.add(w1);
        retryQueue.add(w2);
        retryQueue.add(w3);

        assertThat(retryQueue).hasSize(4);

        assertThat(w0).isNotEqualTo(w1);
        assertThat(w1).isNotEqualTo(w2);

        boolean removed = retryQueue.remove(w1);
        assertThat(removed).isTrue();
        assertThat(retryQueue).hasSize(3);

        assertThat(retryQueue).containsNoDuplicates();

        assertThat(retryQueue.contains(w0)).isTrue();
        assertThat(retryQueue.contains(w1)).isFalse();

        assertThat(retryQueue).contains(w0);
        assertThat(retryQueue).containsNoneIn(of(w1));
        assertThat(retryQueue).contains(w2);
    }
}