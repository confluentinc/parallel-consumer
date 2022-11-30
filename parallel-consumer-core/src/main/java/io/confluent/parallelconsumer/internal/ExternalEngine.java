package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ThreadPoolExecutor;

import static io.confluent.csid.utils.StringUtils.msg;

/**
 * Overrides key aspects required in common for other threading engines like Vert.x and Reactor
 */
@Slf4j
public abstract class ExternalEngine<K, V> extends AbstractParallelEoSStreamProcessor<K, V> {

    protected ExternalEngine(final ParallelConsumerOptions<K, V> newOptions) {
        super(newOptions);

        validate(options);
    }

    private void validate(ParallelConsumerOptions options) {
        if (options.isUsingTransactionCommitMode()) {
            throw new IllegalStateException(msg("External engines (such as Vert.x and Reactor) do not support transactions / EoS ({})", ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER));
        }
    }
//
//    /**
//     * @return the number of records to try to get, based on the current count of records outstanding - but unlike core,
//     *         we don't pipeline messages into the executor pool for processing.
//     */
//    protected int getTargetOutForProcessing() {
//        return getOptions().getTargetAmountOfRecordsInFlight();
//    }

    /**
     * The vert.x module doesn't use any thread pool for dispatching work, as the work is all done by the vert.x engine.
     * This thread is only used to dispatch the work to vert.x.
     * <p>
     * TODO optimise thread usage by not using any extra thread here at all - go straight from the control thread to
     * vert.x.
     */
    @Override
    protected ThreadPoolExecutor setupWorkerPool(int poolSize) {
        return super.setupWorkerPool(1);
    }


}
