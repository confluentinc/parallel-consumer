package io.confluent.csid.asyncconsumer.vertx;

import io.confluent.csid.asyncconsumer.AsyncConsumer;
import io.confluent.csid.asyncconsumer.AsyncConsumerOptions;
import io.confluent.csid.asyncconsumer.WorkContainer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.awaitility.Awaitility.await;

@Slf4j
public class VertxAsyncConsumer<K, V> extends AsyncConsumer<K, V> {

    private static final String VERTX_TYPE = "vert.x type";

    final private Vertx vertx;

    final private WebClient webClient;
    private Optional<Runnable> onVertxCompleteHook = Optional.empty();

    public VertxAsyncConsumer(org.apache.kafka.clients.consumer.Consumer<K, V> consumer, Producer<K, V> producer, AsyncConsumerOptions options) {
        this(consumer, producer, Vertx.vertx(), null, options);
    }

    public VertxAsyncConsumer(org.apache.kafka.clients.consumer.Consumer<K, V> consumer, Producer<K, V> producer, Vertx vertx, WebClient webClient, AsyncConsumerOptions options) {
        super(consumer, producer, options);
        if (vertx == null)
            vertx = Vertx.vertx();
        this.vertx = vertx;
        if (webClient == null)
            webClient = WebClient.create(vertx);
        this.webClient = webClient;
    }

    /**
     * Run any Vertx vertical for message processing
     */
    public void vertical() {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public void close(Duration timeout, boolean waitForInFlight) {
        log.info("Vert.x async consumer closing...");
        waitForNoInFlight(timeout);
        super.close(timeout, waitForInFlight);
        webClient.close();
        Future<Void> close = vertx.close();
        await().until(close::isComplete);
    }

    /**
     * Just give us the {@link RequestInfo}, we'll do the rest.
     * <p>
     * Useful for when the web request is very straight forward.
     *
     * @param requestInfoFunction
     * @param onWebRequestComplete
     */
    public void vertxHttpReqInfo(Function<ConsumerRecord<K, V>, RequestInfo> requestInfoFunction,
                                 Consumer<Future<HttpResponse<Buffer>>> onSend,
                                 Consumer<AsyncResult<HttpResponse<Buffer>>> onWebRequestComplete) {
        vertxHttpRequest((WebClient wc, ConsumerRecord<K, V> rec) -> {
            RequestInfo reqInf = requestInfoFunction.apply(rec);
            HttpRequest<Buffer> req = wc.get(reqInf.getPort(), reqInf.getHost(), reqInf.getContextPath());
            Map<String, String> params = reqInf.getParams();
            for (var entry : params.entrySet()) {
                req = req.addQueryParam(entry.getKey(), entry.getValue());
            }
            return req;
        }, onSend, onWebRequestComplete);
    }

    /**
     * Give us the {@link HttpRequest}, we'll do the rest.
     *
     * @param webClientRequestFunction
     * @param onWebRequestComplete
     */
    public void vertxHttpRequest(BiFunction<WebClient, ConsumerRecord<K, V>, HttpRequest<Buffer>> webClientRequestFunction,
                                 Consumer<Future<HttpResponse<Buffer>>> onSend,
                                 Consumer<AsyncResult<HttpResponse<Buffer>>> onWebRequestComplete) { // TODO remove, redundant over onSend?

        vertxHttpWebClient((webClient, record) -> {
            HttpRequest<Buffer> call = webClientRequestFunction.apply(webClient, record);

            Future<HttpResponse<Buffer>> send = call.send(); // dispatches the work to vertx

            // hook in the users' call back for when the web request get's sent
            send.onComplete(ar -> {
                onWebRequestComplete.accept(ar);
            });

            return send;
        }, onSend);
    }

    /**
     * Initiate your own {@link HttpRequest#send()} call, give us the {@link Future}.
     * <p>
     * Useful for when the request if complicated and needs to be handled in a special way.
     * <p>
     * Note that an alternative is to pass into the constructor a configured {@link WebClient} instead.
     *
     * @param webClientRequestFunction
     */
    public void vertxHttpWebClient(BiFunction<WebClient, ConsumerRecord<K, V>, Future<HttpResponse<Buffer>>> webClientRequestFunction,
                                   Consumer<Future<HttpResponse<Buffer>>> onSend) {

        Function<ConsumerRecord<K, V>, List<Future<HttpResponse<Buffer>>>> userFuncWrapper = (record) -> {
            Future<HttpResponse<Buffer>> send = webClientRequestFunction.apply(webClient, record);

            // send callback
            onSend.accept(send);

            // attach internal handler
            WorkContainer<K, V> wc = wm.getWorkContainerForRecord(record); // todo thread safe?
            wc.setWorkType(VERTX_TYPE);

            send.onSuccess(h -> {
                log.debug("Vert.x Vertical success");
                log.trace("Response body: {}", h.bodyAsString());
                // todo stream http result
                wc.onUserFunctionSuccess();
                addToMailbox(wc);
            });
            send.onFailure(h -> {
                log.error("Vert.x Vertical fail: {}", h.getMessage());
                wc.onUserFunctionFailure();
                addToMailbox(wc);
            });

            // add plugin callback hook
            send.onComplete(ar -> {
                log.trace("Running plugin hook");
                this.onVertxCompleteHook.ifPresent(Runnable::run);
            });

            return List.of(send);
        };

        Consumer<Future<HttpResponse<Buffer>>> noOp = (ignore) -> {
        }; // don't need it, we attach to vertx futures for callback

        super.mainLoop(userFuncWrapper, noOp);
    }

    protected void callInner(Consumer<ConsumerRecord<K, V>> userFuncWrapper) {
        super.asyncPoll(userFuncWrapper);
    }

    public void addVertxOnCompleteHook(Runnable hookFunc) {
        this.onVertxCompleteHook = Optional.of(hookFunc);
    }

    /**
     * Basic information to perform a web request.
     */
    @Setter
    @Getter
    @AllArgsConstructor
    public static class RequestInfo {
        public static final int DEFAULT_PORT = 8080;
        final private String host;
        final private int port;
        final private String contextPath;
        private Map<String, String> params;

        public RequestInfo(String host, String contextPath, Map<String, String> params) {
            this(host, DEFAULT_PORT, contextPath, params);
        }

        public RequestInfo(String host, String contextPath) {
            this(host, DEFAULT_PORT, contextPath, Map.of());
        }

    }

    @Override
    protected void onUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        // with vertx, a function hasn't succeeded until the inner vertx function has also succeeded
        // logging
        if (isVertxWork(resultsFromUserFunction)) {
            log.debug("Vertx creation function success, user's function success");
        } else {
            super.onUserFunctionSuccess(wc, resultsFromUserFunction);
        }
    }

    @Override
    protected void addToMailBoxOnUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        // with vertx, a function hasn't succeeded until the inner vertx function has also succeeded
        // no op
        if (isVertxWork(resultsFromUserFunction)) {
            log.debug("User function success but not adding vertx vertical to mailbox yet");
        } else {
            super.addToMailBoxOnUserFunctionSuccess(wc, resultsFromUserFunction);
        }
    }

    private boolean isVertxWork(List<?> resultsFromUserFunction) {
        for (Object object : resultsFromUserFunction) {
            return (object instanceof Future);
        }
        return false;
    }

}