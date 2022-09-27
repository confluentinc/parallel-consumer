package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.FakeRuntimeError;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.RecordContext;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.threeten.extra.MutableClock;

import java.time.Duration;
import java.util.function.Function;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static org.mockito.Mockito.mock;

class WorkContainerTest {

    @Test
    void basics() {
        var workContainer = new ModelUtils(new PCModuleTestEnv()).createWorkFor(0);
        assertThat(workContainer).getDelayUntilRetryDue().isNotNegative();
    }

    @Test
    void retryDelayProvider() {
        Function<RecordContext<String, String>, Duration> retryDelayProvider = context -> {
            int numberOfFailedAttempts = context.getNumberOfFailedAttempts();
            return Duration.ofSeconds(numberOfFailedAttempts);
        };

        //
        var opts = ParallelConsumerOptions.<String, String>builder()
                .retryDelayProvider(retryDelayProvider)
                .build();

        WorkContainer<String, String> wc = new WorkContainer<String, String>(0,
                mock(ConsumerRecord.class),
                opts.getRetryDelayProvider(),
                MutableClock.epochUTC());

        //
        int numberOfFailures = 3;
        wc.onUserFunctionFailure(new FakeRuntimeError(""));
        wc.onUserFunctionFailure(new FakeRuntimeError(""));
        wc.onUserFunctionFailure(new FakeRuntimeError(""));

        //
        Duration retryDelayConfig = wc.getRetryDelayConfig();

        //
        assertThat(retryDelayConfig).getSeconds().isEqualTo(numberOfFailures);
    }
}
