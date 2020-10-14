package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.parallelconsumer.DrainingCloseable;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Result streaming version of {@link VertxParallelEoSStreamProcessor}.
 */
public interface JStreamVertxParallelStreamProcessor<K, V> extends DrainingCloseable {

    static <KK, VV> JStreamVertxParallelStreamProcessor<KK, VV> createEosStreamProcessor(
            org.apache.kafka.clients.consumer.Consumer<KK, VV> consumer,
            org.apache.kafka.clients.producer.Producer<KK, VV> producer,
            ParallelConsumerOptions options) {
        return new JStreamVertxParallelEoSStreamProcessor<>(consumer, producer, options);
    }

    /**
     * Streaming version
     *
     * @see VertxParallelEoSStreamProcessor#vertxHttpReqInfo
     */
    Stream<JStreamVertxParallelEoSStreamProcessor.VertxCPResult<K, V>> vertxHttpReqInfoStream(Function<ConsumerRecord<K, V>,
            VertxParallelEoSStreamProcessor.RequestInfo> requestInfoFunction);

    /**
     * Streaming version
     *
     * @see VertxParallelEoSStreamProcessor#vertxHttpRequest
     */
    Stream<JStreamVertxParallelEoSStreamProcessor.VertxCPResult<K, V>> vertxHttpRequestStream(BiFunction<WebClient,
            ConsumerRecord<K, V>, HttpRequest<Buffer>> webClientRequestFunction);

    /**
     * Streaming version
     *
     * @see VertxParallelEoSStreamProcessor#vertxHttpWebClient
     */
    Stream<JStreamVertxParallelEoSStreamProcessor.VertxCPResult<K, V>> vertxHttpWebClientStream(
            BiFunction<WebClient, ConsumerRecord<K, V>, Future<HttpResponse<Buffer>>> webClientRequestFunction);
}
