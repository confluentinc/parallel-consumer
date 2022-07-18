package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.InternalRuntimeError;
import io.confluent.parallelconsumer.internal.ProducerManager;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.confluent.parallelconsumer.internal.UserFunctions.carefullyRun;

@Slf4j
public class ParallelEoSStreamProcessor<K, V> extends AbstractParallelEoSStreamProcessor<K, V>
        implements ParallelStreamProcessor<K, V> {

    /**
     * Construct the AsyncConsumer by wrapping this passed in consumer and producer, which can be configured any which
     * way as per normal.
     *
     * @see ParallelConsumerOptions
     */
    public ParallelEoSStreamProcessor(final ParallelConsumerOptions<K, V> newOptions) {
        super(newOptions);
    }

    @Override
    public void poll(Consumer<PollContext<K, V>> usersVoidConsumptionFunction) {
        Function<PollContextInternal<K, V>, ConsumeProduceResult<K, V>> wrappedUserFunc = context -> {
            log.trace("asyncPoll - Consumed a consumerRecord ({}), executing void function...", context);

            carefullyRun(usersVoidConsumptionFunction, context.getPollContext());

            log.trace("asyncPoll - user function finished ok.");
//            Runnable noop = () -> {};
//            Future<RecordMetadata> noopFuture = new FutureTask<>(noop, null);
            return new ConsumeProduceResult<>(context, UniLists.of());
//            return UniLists.of(); // user function returns no produce records, so we satisfy our api
        };
        Consumer<ConsumeProduceResult<K, V>> voidCallBack = ignore -> log.trace("Void callback applied.");
        supervisorLoop(wrappedUserFunc, voidCallBack);
    }

    /**
     * todo docs
     * <p>
     * Records are committed in a block when using batch, so system waits until all produced records for a batch are
     * ack'd, before committing any of the offsets.
     */
    @Override
    @SneakyThrows
    public void pollAndProduceMany(Function<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction,
                                   Consumer<ConsumeProduceResult<K, V>> callback) {
        // todo refactor out the producer system to a sub class
        if (!getOptions().isProducerSupplied()) {
            throw new IllegalArgumentException("To use the produce flows you must supply a Producer in the options");
        }

        // wrap user func to add produce function
        Function<PollContextInternal<K, V>, ConsumeProduceResult<K, V>> wrappedUserFunc
                = context -> {
            ConsumeProduceResult<K, V> results = getConsumeProduceResults(userFunction, context);
            super.handleFutureProduceResultsAsync(results);
            return results;
        };

        super.supervisorLoop(wrappedUserFunc, callback);
    }

    private ConsumeProduceResult<K, V> getConsumeProduceResults(Function<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction,
                                                                PollContextInternal<K, V> context) {
        //
        List<ProducerRecord<K, V>> recordListToProduce = carefullyRun(userFunction, context.getPollContext());

        if (recordListToProduce.isEmpty()) {
            log.debug("No result returned from function to send.");
        }
        log.trace("asyncPoll and Stream - Consumed a record ({}), and returning a derivative result record to be produced: {}", context, recordListToProduce);

        log.trace("Producing {} messages in result...", recordListToProduce.size());


        Callback callback = (RecordMetadata metadata, Exception exception) -> {
            sendRecordResultSuccess(context, metadata, exception);
        };

        Consumer<CallbackArgs<K, V>> call = (CallbackArgs<K, V> args) -> {
            sendRecordResultSuccess(context, args.sent, args.metadata, args.exception);
        };

        // should be three stages so can batch when there's more than one, otherwise it's acquires the read lock N times
        ProducerManager<K, V> pm = super.getProducerManager().get();
        var produceLock = pm.startProducing();
        try {
            var futures = pm.produceMessages(call, recordListToProduce);
            return new ConsumeProduceResult<>(context, futures);
        } catch (Exception e) {
            throw new InternalRuntimeError("Error while waiting for produce results", e);
        } finally {
            pm.finishProducing(produceLock);
        }
    }

    @Value
    public static class CallbackArgs<K, V> {
        RecordMetadata metadata;
        Exception exception;
        ProducerRecord<K, V> sent;
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
                               Consumer<ConsumeProduceResult<K, V>> callback) {
        pollAndProduceMany(consumerRecord -> UniLists.of(userFunction.apply(consumerRecord)), callback);
    }

}
