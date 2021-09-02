package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An extension to {@link AbstractParallelEoSStreamProcessor} which uses the <a href="https://vertx.io">Vert.x</a>
 * library and it's non blocking clients to process messages.
 *
 * @param <K>
 * @param <V>
 * @see AbstractParallelEoSStreamProcessor
 * @see #vertxHttpReqInfo(Function, Consumer, Consumer)
 */
public interface VertxParallelStreamProcessor<K, V> extends ParallelConsumer<K, V> {

    static <KK, VV> VertxParallelStreamProcessor<KK, VV> createEosStreamProcessor(ParallelConsumerOptions options) {
        return new VertxParallelEoSStreamProcessor<>(options);
    }

    /**
     * Consume from the broker concurrently, just give us the {@link VertxParallelEoSStreamProcessor.RequestInfo}, we'll
     * do the rest.
     * <p>
     * Useful for when the web request is very straight forward.
     *
     * @param requestInfoFunction  a function taking a {@link ConsumerRecord} and returns a {@link
     *                             VertxParallelEoSStreamProcessor.RequestInfo} object
     * @param onSend               function executed after the request has been sent
     * @param onWebRequestComplete function executed when response received for request
     */
    void vertxHttpReqInfo(Function<ConsumerRecord<K, V>, VertxParallelEoSStreamProcessor.RequestInfo> requestInfoFunction,
                          Consumer<Future<HttpResponse<Buffer>>> onSend,
                          Consumer<AsyncResult<HttpResponse<Buffer>>> onWebRequestComplete);

    /**
     * Consume from the broker concurrently, give us the {@link HttpRequest}, we'll do the rest.
     *
     * @param webClientRequestFunction Given the {@link WebClient} and a {@link ConsumerRecord}, return us the {@link
     *                                 HttpRequest}
     * @param onSend
     * @param onWebRequestComplete
     */
    void vertxHttpRequest(BiFunction<WebClient, ConsumerRecord<K, V>, HttpRequest<Buffer>> webClientRequestFunction,
                          Consumer<Future<HttpResponse<Buffer>>> onSend,
                          Consumer<AsyncResult<HttpResponse<Buffer>>> onWebRequestComplete);

    /**
     * Consume from the broker concurrently, initiating your own {@link HttpRequest#send()} call, give us the {@link
     * Future}.
     * <p>
     * Useful for when the request if complicated and needs to be handled in a special way.
     * <p>
     * Note that an alternative is to pass into the constructor a configured {@link WebClient} instead.
     *
     * @see #vertxHttpReqInfo
     * @see #vertxHttpRequest
     */
    void vertxHttpWebClient(BiFunction<WebClient, ConsumerRecord<K, V>, Future<HttpResponse<Buffer>>> webClientRequestFunction,
                            Consumer<Future<HttpResponse<Buffer>>> onSend);

    /**
     * Consumer from the Broker concurrently - use the various Vert.x systems to return us a vert.x Future based on this
     * record.
     */
    void vertxFuture(final Function<ConsumerRecord<K, V>, Future<?>> result);
}
