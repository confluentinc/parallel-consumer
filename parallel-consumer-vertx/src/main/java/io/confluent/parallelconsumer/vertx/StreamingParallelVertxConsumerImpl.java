package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumer;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.csid.utils.Java8StreamUtils;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Result streaming version of {@link VertxParallelConsumerImpl}.
 */
@Slf4j
public class StreamingParallelVertxConsumerImpl<K, V> extends VertxParallelConsumerImpl<K, V> {

    /**
     * The stream of results, constructed from the Queue {@link #userProcessResultsStream}
     */
    final private Stream<VertxCPResult<K, V>> stream;

    /**
     * The Queue of results
     */
    final private ConcurrentLinkedDeque<VertxCPResult<K, V>> userProcessResultsStream;

    /**
     * Provide your own instances of the Vertx engine and it's webclient.
     * <p>
     * Use this to share a Vertx runtime with different systems for efficiency.
     */
    public StreamingParallelVertxConsumerImpl(org.apache.kafka.clients.consumer.Consumer<K, V> consumer,
                                              Producer<K, V> producer,
                                              Vertx vertx,
                                              WebClient webClient,
                                              ParallelConsumerOptions options) {
        super(consumer, producer, vertx, webClient, options);

        userProcessResultsStream = new ConcurrentLinkedDeque<>();

        stream = Java8StreamUtils.setupStreamFromDeque(userProcessResultsStream);
    }

    /**
     * Simple constructor. Internal Vertx objects will be created.
     */
    public StreamingParallelVertxConsumerImpl(org.apache.kafka.clients.consumer.Consumer<K, V> consumer,
                                              Producer<K, V> producer,
                                              ParallelConsumerOptions options) {
        this(consumer, producer, null, null, options);
    }
    
    /**
     * Streaming version
     * 
     * @see VertxParallelConsumerImpl#vertxHttpReqInfo
     */
    public Stream<VertxCPResult<K, V>> vertxHttpReqInfoStream(Function<ConsumerRecord<K, V>, RequestInfo> requestInfoFunction) {

        VertxCPResult.VertxCPResultBuilder<K, V> result = VertxCPResult.builder();

        Function<ConsumerRecord<K, V>, RequestInfo> requestInfoFunctionWrapped = x -> {
            result.in(x);
            RequestInfo apply = requestInfoFunction.apply(x);
            result.requestInfo(Optional.of(apply));
            return apply;
        };

        Consumer<Future<HttpResponse<Buffer>>> onSendCallBack = future -> {
            // stream
            result.asr(future);
            VertxCPResult<K, V> build = result.build();
            userProcessResultsStream.add(build);
        };

        super.vertxHttpReqInfo(requestInfoFunctionWrapped, onSendCallBack, (ignore) -> {
        });

        return stream;
    }

    /**
     * Streaming version
     *
     * @see VertxParallelConsumerImpl#vertxHttpRequest
     */
    public Stream<VertxCPResult<K, V>> vertxHttpRequestStream(BiFunction<WebClient, ConsumerRecord<K, V>, HttpRequest<Buffer>> webClientRequestFunction) {

        VertxCPResult.VertxCPResultBuilder<K, V> result = VertxCPResult.builder();

        BiFunction<WebClient, ConsumerRecord<K, V>, HttpRequest<Buffer>> requestInfoFunctionWrapped = (wc, x) -> {
            result.in(x);
            HttpRequest<Buffer> apply = webClientRequestFunction.apply(wc, x);
            result.httpReq(Optional.of(apply));
            return apply;
        };

        Consumer<Future<HttpResponse<Buffer>>> onSendCallBack = future -> {
            // stream
            result.asr(future);
            VertxCPResult<K, V> build = result.build();
            userProcessResultsStream.add(build);
        };

        super.vertxHttpRequest(requestInfoFunctionWrapped, onSendCallBack, (ignore) -> {
        });
        return stream;
    }

    /**
     * Streaming version
     *
     * @see VertxParallelConsumerImpl#vertxHttpWebClient
     */
    public Stream<VertxCPResult<K, V>> vertxHttpWebClientStream(
            BiFunction<WebClient, ConsumerRecord<K, V>, Future<HttpResponse<Buffer>>> webClientRequestFunction) {

        VertxCPResult.VertxCPResultBuilder<K, V> result = VertxCPResult.builder();

        BiFunction<WebClient, ConsumerRecord<K, V>, Future<HttpResponse<Buffer>>> wrappedFunc = (x, y) -> {
            // capture
            result.in(y);
            Future<HttpResponse<Buffer>> apply = webClientRequestFunction.apply(x, y);
            result.asr(apply);
            return apply;
        };

        Consumer<Future<HttpResponse<Buffer>>> onSendCallBack = future -> {
            // stream
            result.asr(future);
            VertxCPResult<K, V> build = result.build();
            userProcessResultsStream.add(build);
        };

        super.vertxHttpWebClient(wrappedFunc, onSendCallBack);

        return stream;
    }

    /**
     * @param <K>
     * @param <V>
     * @see ParallelConsumer.ConsumeProduceResult
     */
    @Getter
    @Builder
    public static class VertxCPResult<K, V> {
        final private ConsumerRecord<K, V> in;
        final private Future<HttpResponse<Buffer>> asr;

        // todo change to class generic type variables? 2 fields become 1. Not worth the hassle atm.
        // Currently our vertx usage has two types of results. This is a quick way to model that. Should consider improving.
        @Builder.Default
        final private Optional<RequestInfo> requestInfo = Optional.empty();
        @Builder.Default
        final private Optional<HttpRequest<Buffer>> httpReq = Optional.empty();
    }

}