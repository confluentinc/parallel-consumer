package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.internal.ExternalEngineRunner;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.vertx.core.Future;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author Antony Stubbs
 */
@Slf4j
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class VertxRunner<K, V, R> extends ExternalEngineRunner<K, V, R> {

    /**
     * Determines if any of the elements in the supplied list is a Vertx Future type
     */
    @Override
    protected boolean isAsyncFutureWork(List<?> resultsFromUserFunction) {
        for (Object object : resultsFromUserFunction) {
            return (object instanceof Future);
        }
        return false;
    }

    @Override
    protected void onUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        // with vertx, a function hasn't succeeded until the inner vertx function has also succeeded
        // logging
        if (isAsyncFutureWork(resultsFromUserFunction)) {
            log.debug("Vertx creation function success, user's function success");
        } else {
            super.onUserFunctionSuccess(wc, resultsFromUserFunction);
        }
    }

    @Override
    protected void addToMailBoxOnUserFunctionSuccess(final PollContextInternal<K, V> context, WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        // with vertx, a function hasn't succeeded until the inner vertx function has also succeeded
        // no op
        if (isAsyncFutureWork(resultsFromUserFunction)) {
            log.debug("User function success but not adding vertx vertical to mailbox yet");
        } else {
            super.addToMailBoxOnUserFunctionSuccess(context, wc, resultsFromUserFunction);
        }
    }

}
