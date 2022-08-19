package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.TimeUtils;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.InternalRuntimeError;
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
import java.util.function.Consumer;
import java.util.function.Function;

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
    public ParallelEoSStreamProcessor(final ParallelConsumerOptions<K, V> newOptions) {
        super(newOptions);
    }

    @Override
    public void poll(Consumer<PollContext<K, V>> usersVoidConsumptionFunction) {
        Function<PollContextInternal<K, V>, List<Object>> wrappedUserFunc = (context) -> {
            log.trace("asyncPoll - Consumed a consumerRecord ({}), executing void function...", context);

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
        // todo refactor out the producer system to a sub class
        if (!getOptions().isProducerSupplied()) {
            throw new IllegalArgumentException("To use the produce flows you must supply a Producer in the options");
        }

        // wrap user func to add produce function
        Function<PollContextInternal<K, V>, List<ConsumeProduceResult<K, V, K, V>>> wrappedUserFunc
                = context -> {

            // todo refactor this block into #getConsumeProduceResults at end of review or in seperate commit / pr
            {
                //
                List<ProducerRecord<K, V>> recordListToProduce = carefullyRun(userFunction, context.getPollContext());

                if (recordListToProduce.isEmpty()) {
                    log.debug("No result returned from function to send.");
                }
                log.trace("asyncPoll and Stream - Consumed a record ({}), and returning a derivative result record to be produced: {}", context, recordListToProduce);

                List<ConsumeProduceResult<K, V, K, V>> results = new ArrayList<>();
                log.trace("Producing {} messages in result...", recordListToProduce.size());

                // by having the produce lock span the block on acks, means starting a commit cycle blocks until ack wait is finished
                ProducerManager<K, V> pm = super.getProducerManager().get();
                var produceLock = pm.beginProducing();
                context.setProducingLock(of(produceLock));

                // wait for all ack's to complete, see PR #356 for async version
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
                    throw new InternalRuntimeError("Error while waiting for produce results", e);
                }
                return results;
            }
        };

        supervisorLoop(wrappedUserFunc, callback);
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
