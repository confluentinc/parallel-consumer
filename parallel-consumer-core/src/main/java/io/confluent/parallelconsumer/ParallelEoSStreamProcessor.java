package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.TimeUtils;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.InternalRuntimeException;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.internal.ProducerManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.internal.UserFunctions.carefullyRun;
import static java.util.Optional.of;

@Slf4j
public class ParallelEoSStreamProcessor<K, V> extends AbstractParallelEoSStreamProcessor<K, V>
        implements ParallelStreamProcessor<K, V> {

    /**
     * Construct the AsyncConsumer by wrapping this passed in consumer and producer, which can be configured any which
     * way as per normal.
     *
     * @see ParallelConsumerOptions
     */
    public ParallelEoSStreamProcessor(ParallelConsumerOptions<K, V> newOptions, PCModule<K, V> module) {
        super(newOptions, module);
    }

    public ParallelEoSStreamProcessor(ParallelConsumerOptions<K, V> newOptions) {
        super(newOptions);
    }

    @Override
    public void poll(Consumer<PollContext<K, V>> usersVoidConsumptionFunction) {
        Function<PollContextInternal<K, V>, List<Object>> wrappedUserFunc = (context) -> {
            log.trace("Consumed a consumerRecord ({}), executing void function...", context);

            carefullyRun(usersVoidConsumptionFunction, context.getPollContext());

            log.trace("asyncPoll - user function finished ok.");
            return UniLists.of(); // user function returns no produce records, so we satisfy our api
        };
        Consumer<Object> voidCallBack = ignore -> log.trace("Void callback applied.");
        supervisorLoop(wrappedUserFunc, voidCallBack);
    }

    @Override
    @SneakyThrows
    public void pollAndProduceMany(Function<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction,
                                   Consumer<ConsumeProduceResult<K, V, K, V>> callback) {
        if (!getOptions().isProducerSupplied()) {
            throw new IllegalArgumentException("To use the produce flows you must supply a Producer in the options");
        }

        // wrap user func to add produce function
        Function<PollContextInternal<K, V>, List<ConsumeProduceResult<K, V, K, V>>> producingUserFunctionWrapper =
                context -> processAndProduceResults(userFunction, context);

        supervisorLoop(producingUserFunctionWrapper, callback);
    }

    /**
     * todo refactor to it's own class, so that the wrapping function can be used directly from
     *  tests, e.g. see: {@see ProducerManagerTest#producedRecordsCantBeInTransactionWithoutItsOffsetDirect}
     */
    private List<ConsumeProduceResult<K, V, K, V>> processAndProduceResults(final Function<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction,
                                                                            final PollContextInternal<K, V> context) {
        ProducerManager<K, V> pm = super.getProducerManager().get();

        // if running strict with no processing during commit - get the produce lock first
        if (options.isUsingTransactionCommitMode() && !options.isAllowEagerProcessingDuringTransactionCommit()) {
            try {
                ProducerManager<K, V>.ProducingLock produceLock = pm.beginProducing(context);
                context.setProducingLock(of(produceLock));
            } catch (TimeoutException e) {
                throw new RuntimeException(msg("Timeout trying to early acquire produce lock to send record in {} mode - could not START record processing phase", PERIODIC_TRANSACTIONAL_PRODUCER), e);
            }
        }


        // run the user function, which is expected to return records to be sent
        List<ProducerRecord<K, V>> recordListToProduce = carefullyRun(userFunction, context.getPollContext());

        //
        if (recordListToProduce.isEmpty()) {
            log.debug("No result returned from function to send.");
            return UniLists.of();
        }
        log.trace("asyncPoll and Stream - Consumed a record ({}), and returning a derivative result record to be produced: {}", context, recordListToProduce);

        List<ConsumeProduceResult<K, V, K, V>> results = new ArrayList<>();
        log.trace("Producing {} messages in result...", recordListToProduce.size());

        // by having the produce lock span the block on acks, means starting a commit cycle blocks until ack wait is finished
        if (options.isUsingTransactionCommitMode() && options.isAllowEagerProcessingDuringTransactionCommit()) {
            try {
                ProducerManager<K, V>.ProducingLock produceLock = pm.beginProducing(context);
                context.setProducingLock(of(produceLock));
            } catch (TimeoutException e) {
                throw new RuntimeException(msg("Timeout trying to late acquire produce lock to send record in {} mode", PERIODIC_TRANSACTIONAL_PRODUCER), e);
            }
        }

        // wait for all acks to complete, see PR #356 for a fully async version which doesn't need to block here
        try {
            var futures = pm.produceMessages(recordListToProduce);

            TimeUtils.time(() -> {
                for (var futureTuple : futures) {
                    Future<RecordMetadata> futureSend = futureTuple.getRight();

                    var recordMetadata = futureSend.get(options.getSendTimeout().toMillis(), TimeUnit.MILLISECONDS);

                    var result = new ConsumeProduceResult<>(context.getPollContext(), futureTuple.getLeft(), recordMetadata);
                    results.add(result);
                }
                return null; // return from timer function
            });
        } catch (Exception e) {
            throw new InternalRuntimeException("Error while waiting for produce results", e);
        }
        return results;
    }

    @Override
    @SneakyThrows
    public void pollAndProduceMany(Function<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction) {
        pollAndProduceMany(userFunction, consumerRecord -> {
            // no op call back
            log.trace("No-op user callback");
        });
    }

    @Override
    @SneakyThrows
    public void pollAndProduce(Function<PollContext<K, V>, ProducerRecord<K, V>> userFunction) {
        pollAndProduce(userFunction, consumerRecord -> {
            // no op call back
            log.trace("No-op user callback");
        });
    }

    @Override
    @SneakyThrows
    public void pollAndProduce(Function<PollContext<K, V>, ProducerRecord<K, V>> userFunction,
                               Consumer<ConsumeProduceResult<K, V, K, V>> callback) {
        pollAndProduceMany(consumerRecord -> UniLists.of(userFunction.apply(consumerRecord)), callback);
    }

}
