package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@SuperBuilder
public abstract class ExternalEngineRunner<K, V, R> extends FunctionRunner<K, V, R> {

    /**
     * System dependent way for the core engine to know if an external scheduler needs to be relied on for completion,
     * as opposed to the internal threading system in the core module.
     *
     * @param resultsFromUserFunction the results to test
     * @return true if the work needs special treatment
     */
    // TODO: Now that the modules don't use the internal threading systems at all, is this method redundant as all work from a module extension would return true
    protected abstract boolean isAsyncFutureWork(List<?> resultsFromUserFunction);

    /**
     * With Vertx and Reactor, a function hasn't succeeded until the inner vertx function has also succeeded logging
     */
    @Override
    protected void onUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        if (isAsyncFutureWork(resultsFromUserFunction)) {
            log.debug("Reactor creation function success, user's function success");
        } else {
            super.onUserFunctionSuccess(wc, resultsFromUserFunction);
        }
    }

    /**
     * With Vertx and Reactor, a function hasn't succeeded until the inner vertx function has also succeeded no op
     */
    @Override
    protected void addToMailBoxOnUserFunctionSuccess(final PollContextInternal<K, V> context, WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        if (isAsyncFutureWork(resultsFromUserFunction)) {
            log.debug("User function success but not adding vertx vertical to mailbox yet");
        } else {
            super.addToMailBoxOnUserFunctionSuccess(context, wc, resultsFromUserFunction);
        }
    }
}
