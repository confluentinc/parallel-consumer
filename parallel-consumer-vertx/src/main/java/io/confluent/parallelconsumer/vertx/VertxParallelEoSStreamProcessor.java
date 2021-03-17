package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.internal.ExternalEngine;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Time;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.confluent.parallelconsumer.internal.UserFunctions.carefullyRun;


@Slf4j
public class VertxParallelEoSStreamProcessor<K, V> extends ExternalEngine<K, V>
        implements VertxParallelStreamProcessor<K, V> {

    /**
     * @see WorkContainer#getWorkType()
     */
    private static final String VERTX_TYPE = "vert.x-type";

    /**
     * The Vertx engine to use
     */
    private final Vertx vertx;

    /**
     * The Vertx webclient for making HTTP requests
     */
    private final WebClient webClient;

    /**
     * Extension point for running after Vertx {@link io.vertx.core.Verticle}s finish.
     */
    private Optional<Runnable> onVertxCompleteHook = Optional.empty();

    /**
     * Simple constructor. Internal Vertx objects will be created.
     */
    public VertxParallelEoSStreamProcessor(ParallelConsumerOptions options) {
        this(Vertx.vertx(), null, options);
    }

    /**
     * Provide your own instances of the Vertx engine and it's webclient.
     * <p>
     * Use this to share a Vertx runtime with different systems for efficiency, or to customise configuration.
     * <p>
     * By default Vert.x's {@link WebClient} uses quite small connection limits to servers. PC overrides this to {@link
     * ParallelConsumerOptions#getMaxConcurrency()}. You can configure these yourself by providing a configured Vert.x
     * {@link WebClient} with {@link WebClientOptions} set to how you please. Consider also looking at other options
     * below.
     *
     * @see WebClientOptions#setMaxPoolSize
     * @see WebClientOptions#setMaxWaitQueueSize(int)
     * @see WebClientOptions#setPipelining(boolean)
     * @see WebClientOptions#setPipeliningLimit(int)
     * @see WebClientOptions#setHttp2MaxPoolSize(int)
     * @see WebClientOptions#setHttp2MultiplexingLimit(int)
     */
    public VertxParallelEoSStreamProcessor(Vertx vertx,
                                           WebClient webClient,
                                           ParallelConsumerOptions options) {
        super(options);

        int cores = Runtime.getRuntime().availableProcessors();
        VertxOptions vertxOptions = new VertxOptions().setWorkerPoolSize(cores);

        int maxConcurrency = options.getMaxConcurrency();

        // should this be user configurable? - probably
        WebClientOptions webClientOptions = new WebClientOptions()
                .setMaxPoolSize(maxConcurrency) // defaults to 5
                .setHttp2MaxPoolSize(maxConcurrency) // defaults to 1
                ;

        if (vertx == null)
            vertx = Vertx.vertx(vertxOptions);
        this.vertx = vertx;
        if (webClient == null)
            webClient = WebClient.create(vertx, webClientOptions);
        this.webClient = webClient;
    }

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

    @Override
    public void vertxHttpReqInfo(Function<ConsumerRecord<K, V>, RequestInfo> requestInfoFunction,
                                 Consumer<Future<HttpResponse<Buffer>>> onSend,
                                 Consumer<AsyncResult<HttpResponse<Buffer>>> onWebRequestComplete) {
        vertxHttpRequest((WebClient webClient, ConsumerRecord<K, V> rec) -> {
            RequestInfo reqInf = carefullyRun(requestInfoFunction, rec);

            HttpRequest<Buffer> req = webClient.get(reqInf.getPort(), reqInf.getHost(), reqInf.getContextPath());
            Map<String, String> params = reqInf.getParams();
            for (var entry : params.entrySet()) {
                req = req.addQueryParam(entry.getKey(), entry.getValue());
            }
            return req;
        }, onSend, onWebRequestComplete);
    }

    @Override
    public void vertxHttpRequest(BiFunction<WebClient, ConsumerRecord<K, V>, HttpRequest<Buffer>> webClientRequestFunction,
                                 Consumer<Future<HttpResponse<Buffer>>> onSend,
                                 Consumer<AsyncResult<HttpResponse<Buffer>>> onWebRequestComplete) {

        vertxHttpWebClient((webClient, record) -> {
            HttpRequest<Buffer> call = carefullyRun(webClientRequestFunction, webClient, record);

            Future<HttpResponse<Buffer>> send = call.send(); // dispatches the work to vertx

            // hook in the users' call back for when the web request gets a response
            send.onComplete(ar ->
                    onWebRequestComplete.accept(ar)
            );

            return send;
        }, onSend);
    }

    @Override
    public void vertxHttpWebClient(BiFunction<WebClient, ConsumerRecord<K, V>, Future<HttpResponse<Buffer>>> webClientRequestFunction,
                                   Consumer<Future<HttpResponse<Buffer>>> onWebRequestSentCallback) {

        Function<ConsumerRecord<K, V>, List<Future<HttpResponse<Buffer>>>> userFuncWrapper = (record) -> {

            Future<HttpResponse<Buffer>> futureWebResponse = carefullyRun(webClientRequestFunction, webClient, record);

            // execute user's onSend callback
            onWebRequestSentCallback.accept(futureWebResponse);

            addVertxHooks(record, futureWebResponse);

            return UniLists.of(futureWebResponse);
        };

        Consumer<Future<HttpResponse<Buffer>>> noOp = (ignore) -> {
        }; // don't need it, we attach to vertx futures for callback

        super.supervisorLoop(userFuncWrapper, noOp);
    }

    private void addVertxHooks(final ConsumerRecord<K, V> record, final Future<?> send) {
        // attach internal handler
        WorkContainer<K, V> wc = wm.getSm().getWorkContainerForRecord(record);
        wc.setWorkType(VERTX_TYPE);

        send.onSuccess(h -> {
            log.debug("Vert.x Vertical success");
//            log.trace("Response body: {}", h.bodyAsString());
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
    }

    @Override
    public void vertxFuture(final Function<ConsumerRecord<K, V>, Future<?>> result) {

        Function<ConsumerRecord<K, V>, List<Future<?>>> userFuncWrapper = record -> {

            Future<?> send = carefullyRun(result, record);

            addVertxHooks(record, send);

            return UniLists.of(send);
        };

        Consumer<Future<?>> noOp = ignore -> {
        }; // don't need it, we attach to vertx futures for callback

        super.supervisorLoop(userFuncWrapper, noOp);
    }

    /**
     * @see #onVertxCompleteHook
     */
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
        private final String host;
        private final int port;
        private final String contextPath;
        private Map<String, String> params;

        public RequestInfo(String host, String contextPath, Map<String, String> params) {
            this(host, DEFAULT_PORT, contextPath, params);
        }

        public RequestInfo(String host, String contextPath) {
            this(host, DEFAULT_PORT, contextPath, UniMaps.of());
        }

    }

    @Override
    protected void onUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        // with vertx, a function hasn't succeeded until the inner vertx function has also succeeded
        // logging
        if (isAsyncFutureWork(resultsFromUserFunction)) {
            log.debug("Vertx creation function success, user's function success");
        } else {
            super.onUserFunctionSuccess(wc, resultsFromUserFunction);
        }
    }

    @Override
    protected void addToMailBoxOnUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        // with vertx, a function hasn't succeeded until the inner vertx function has also succeeded
        // no op
        if (isAsyncFutureWork(resultsFromUserFunction)) {
            log.debug("User function success but not adding vertx vertical to mailbox yet");
        } else {
            super.addToMailBoxOnUserFunctionSuccess(wc, resultsFromUserFunction);
        }
    }

    /**
     * Determines if any of the elements in the supplied list is a Vertx Future type
     */
    @Override
    protected boolean isAsyncFutureWork(List<?> resultsFromUserFunction) {
        for (Object object : resultsFromUserFunction) {
            return (object instanceof Future);
        }
        return false;
    }

    /**
     * Close the concurrent Vertx consumer system
     *
     * @param timeout   how long to wait before giving up
     * @param drainMode wait for messages already consumed from the broker to be processed before closing
     */
    @SneakyThrows
    @Override
    public void close(Duration timeout, DrainingMode drainMode) {
        log.info("Vert.x async consumer closing...");
        super.close(timeout, drainMode);
        webClient.close();
        Future<Void> close = vertx.close();
        var timer = Time.SYSTEM.timer(timeout);
        while (!close.isComplete()) {
            log.trace("Waiting on close to complete");
            Thread.sleep(100);
            timer.update();
            if (timer.isExpired()) {
                throw new TimeoutException("Waiting for system to close");
            }
        }
    }

}
