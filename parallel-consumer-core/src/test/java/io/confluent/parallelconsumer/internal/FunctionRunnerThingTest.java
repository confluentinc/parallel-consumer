package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.*;
import io.confluent.parallelconsumer.ParallelConsumerOptions.TerminalFailureReaction;
import io.confluent.parallelconsumer.state.ModelUtils;
import io.confluent.parallelconsumer.state.WorkManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;

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

        FunctionRunnerThing<String, String> r = new FunctionRunnerThing<>(mock);
        var workFor = ModelUtils.createWorkFor(0);
        r.runUserFunction(fake,
                o -> {
                }, List.of(workFor));
    }

    @Test
    void testTwo() {
        run(SKIP, context -> {
            throw new PCTerminalException("fake");
        });
    }

    @Test
    void testThree() {
        run(SKIP, context -> {
            throw new PCRetriableException("fake", Offsets.ofLongs(0, 1));
        });
    }

}
