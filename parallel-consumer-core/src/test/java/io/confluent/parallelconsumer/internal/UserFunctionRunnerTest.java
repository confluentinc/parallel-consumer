package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.PCTerminalException;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.TerminalFailureReaction;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.state.ModelUtils;
import io.confluent.parallelconsumer.state.WorkManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.TerminalFailureReaction.SHUTDOWN;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.TerminalFailureReaction.SKIP;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @see UserFunctionRunner
 */
class UserFunctionRunnerTest {

    @Test
    void shutdown() {
        run(SHUTDOWN, context -> {
            throw new PCTerminalException("fake");
        });
    }

    private void run(TerminalFailureReaction shutdown, Function<PollContextInternal<String, String>, List<Object>> fake) {
        var mock = mock(AbstractParallelEoSStreamProcessor.class);
        when(mock.getWm()).thenReturn(mock(WorkManager.class));
        when(mock.getOptions()).thenReturn(ParallelConsumerOptions.builder()
                .terminalFailureReaction(shutdown)
                .build());

        UserFunctionRunner<String, String> r = new UserFunctionRunner<>(mock, Clock.systemUTC(), Optional.empty(), mock(AdminClient.class));
        var workFor = ModelUtils.createWorkFor(0);
        r.runUserFunction(fake,
                o -> {
                }, List.of(workFor));
    }

    @Test
    void skip() {
        run(SKIP, context -> {
            throw new PCTerminalException("fake");
        });
    }

}
