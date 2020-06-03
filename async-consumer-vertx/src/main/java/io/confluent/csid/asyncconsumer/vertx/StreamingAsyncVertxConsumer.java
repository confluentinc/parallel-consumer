package io.confluent.csid.asyncconsumer.vertx;

import io.confluent.csid.asyncconsumer.AsyncConsumerOptions;
import io.confluent.csid.utils.Java8StreamUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.Builder;
import lombok.Data;
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

@Slf4j
public class StreamingAsyncVertxConsumer<K, V> extends VertxAsyncConsumer<K, V> {

    final private Stream<Result<K, V>> stream;

    final private ConcurrentLinkedDeque<Result<K, V>> userProcessResultsStream;

    public StreamingAsyncVertxConsumer(org.apache.kafka.clients.consumer.Consumer<K, V> consumer,
                                       Producer<K, V> producer,
                                       Vertx vertx,
                                       WebClient webClient,
                                       AsyncConsumerOptions options) {
        super(consumer, producer, vertx, webClient, options);

        userProcessResultsStream = new ConcurrentLinkedDeque<>();

        stream = Java8StreamUtils.setupStreamFromDeque(userProcessResultsStream);
    }

    public StreamingAsyncVertxConsumer(org.apache.kafka.clients.consumer.Consumer<K, V> consumer, Producer<K, V> producer) {
        this(consumer, producer, null, null, null);
    }

    @Override
    protected void callInner(Consumer<ConsumerRecord<K, V>> userFuncWrapper) {
        super.asyncPoll(userFuncWrapper);
    }

    /**
     * todo see other
     */
    public Stream<Result<K, V>> vertxHttpReqInfoStream(Function<ConsumerRecord<K, V>, RequestInfo> requestInfoFunction) {

        Result.ResultBuilder<K, V> result = Result.builder();

        Function<ConsumerRecord<K, V>, RequestInfo> requestInfoFunctionWrapped = x -> {
            result.in(x);
            RequestInfo apply = requestInfoFunction.apply(x);
            result.req(Optional.of(apply));
            return apply;
        };


        Consumer<Future<HttpResponse<Buffer>>> onSendCallBack = future -> {
            // stream
            result.asr(future);
            Result<K, V> build = result.build();
            userProcessResultsStream.add(build);
        };

        super.vertxHttpReqInfo(requestInfoFunctionWrapped, onSendCallBack, (ignore) -> {});

        return stream;
    }

    /**
     * todo see other
     */
    public Stream<Result<K, V>> vertxHttpRequestStream(BiFunction<WebClient, ConsumerRecord<K, V>, HttpRequest<Buffer>> webClientRequestFunction) {

        Result.ResultBuilder<K, V> result = Result.builder();

        BiFunction<WebClient, ConsumerRecord<K, V>, HttpRequest<Buffer>> requestInfoFunctionWrapped = (wc, x) -> {
            result.in(x);
            HttpRequest<Buffer> apply = webClientRequestFunction.apply(wc, x);
            result.httpReq(Optional.of(apply));
            return apply;
        };

        Consumer<Future<HttpResponse<Buffer>>> onSendCallBack = future -> {
            // stream
            result.asr(future);
            Result<K, V> build = result.build();
            userProcessResultsStream.add(build);
        };

        super.vertxHttpRequest(requestInfoFunctionWrapped, onSendCallBack, (ignore) -> {});
        return stream;
    }

    /**
     * todo see other
     */
    public Stream<Result<K, V>> vertxHttpWebClientStream(
            BiFunction<WebClient, ConsumerRecord<K, V>, Future<HttpResponse<Buffer>>> webClientRequestFunction) {

        Result.ResultBuilder<K, V> result = Result.builder();

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
            Result<K, V> build = result.build();
            userProcessResultsStream.add(build);
        };

        super.vertxHttpWebClient(wrappedFunc, onSendCallBack);

        return stream;
    }

    @Getter
    @Builder
    public static class Result<K, V> {
        final private ConsumerRecord<K, V> in;
        @Builder.Default
        final private Optional<RequestInfo> req = Optional.empty(); // todo change to type variables? 2 fields become 2, tlc. Not worth the hassle atm.
        @Builder.Default
        final private Optional<HttpRequest> httpReq = Optional.empty();
//        final private AsyncResult<HttpResponse<Buffer>> asr;
        final private Future<HttpResponse<Buffer>> asr;
    }

}