package io.confluent.parallelconsumer.reactor;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import reactor.core.publisher.Flux;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.BaseStream;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;

public class ReactorUnitTestBase extends ParallelEoSStreamProcessorTestBase {

    protected ReactorProcessor<String, String> reactorPC;

    protected static final int MAX_CONCURRENCY = 1000;

    @Override
    protected AbstractParallelEoSStreamProcessor initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        var build = parallelConsumerOptions.toBuilder()
                .commitMode(PERIODIC_CONSUMER_SYNC)
                .maxConcurrency(MAX_CONCURRENCY)
                .build();

        reactorPC = new ReactorProcessor<>(build);

        return reactorPC;
    }

    protected static Flux<String> fromPath(Path path) {
        return Flux.using(() -> Files.lines(path),
                Flux::fromStream,
                BaseStream::close
        );
    }

}
