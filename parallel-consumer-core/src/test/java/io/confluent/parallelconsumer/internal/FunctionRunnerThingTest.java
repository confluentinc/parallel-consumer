package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.Offsets;
import io.confluent.parallelconsumer.PCRetriableException;
import io.confluent.parallelconsumer.PCTerminalException;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.state.ModelUtils;
import io.confluent.parallelconsumer.state.WorkManager;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.TerminalFailureReaction.SHUTDOWN;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.TerminalFailureReaction.SKIP;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @see FunctionRunnerThing
 */
class FunctionRunnerThingTest {

    @Test
    void test() {
        var mock = mock(AbstractParallelEoSStreamProcessor.class);
        when(mock.getWm()).thenReturn(mock(WorkManager.class));
        when(mock.getOptions()).thenReturn(ParallelConsumerOptions.builder()
                .terminalFailureReaction(SHUTDOWN)
                .build());

        FunctionRunnerThing<String, String> r = new FunctionRunnerThing<>(mock);
        var workFor = ModelUtils.createWorkFor(0);
        r.runUserFunction(context -> {
                    throw new PCTerminalException("fake");
                },
                o -> {
                }, List.of(workFor));
    }

    @Test
    void testTwo() {
        var mock = mock(AbstractParallelEoSStreamProcessor.class);
        when(mock.getWm()).thenReturn(mock(WorkManager.class));
        when(mock.getOptions()).thenReturn(ParallelConsumerOptions.builder()
                .terminalFailureReaction(SKIP)
                .build());

        FunctionRunnerThing<String, String> r = new FunctionRunnerThing<>(mock);
        var workFor = ModelUtils.createWorkFor(0);
        r.runUserFunction(context -> {
                    throw new PCTerminalException("fake");
                },
                o -> {
                }, List.of(workFor));
    }

    @Test
    void testThree() {
        var mock = mock(AbstractParallelEoSStreamProcessor.class);
        when(mock.getWm()).thenReturn(mock(WorkManager.class));
        when(mock.getOptions()).thenReturn(ParallelConsumerOptions.builder()
                .terminalFailureReaction(SKIP)
                .build());

        FunctionRunnerThing<String, String> r = new FunctionRunnerThing<>(mock);
        var workFor = ModelUtils.createWorkFor(0);
        r.runUserFunction(context -> {
                    throw new PCRetriableException("fake", Offsets.ofLongs(0, 1));
                },
                o -> {
                }, List.of(workFor));
    }

}
