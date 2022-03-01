package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.internal.ExternalEngine;
import io.confluent.parallelconsumer.state.ShardManager;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Time;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.internal.UserFunctions.carefullyRun;
import static java.time.Duration.ofMinutes;


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

        deployVertical(vertx);
    }

    public static final String PC_VERTX_BATCH_RECORD_LIST = "pc.vertx.batch.recordList";

    private void deployVertical(Vertx vertx) {
        DeploymentOptions options = new DeploymentOptions();
        options.setInstances(Runtime.getRuntime().availableProcessors());
        options.setWorkerPoolName(getMyId().orElse("pc-vertical"));

        vertx.exceptionHandler(event -> {
            RuntimeException runtimeException = new RuntimeException(msg("PC Vert.x system failure: {}", event.getMessage()));
            log.error(runtimeException.getMessage());
            super.failureReason = runtimeException;
        });

        vertx.deployVerticle(() -> new Verticle() {
            @Override
            public Vertx getVertx() {
                return vertx;
            }

            @Override
            public void init(Vertx vertx, Context context) {
                EventBus eventBus = vertx.eventBus();
                eventBus.registerDefaultCodec(BatchWrapper.class, new BatchWrapperCodec());
                eventBus.registerDefaultCodec(ArrayList.class, new ArrayListCodec());
                eventBus.registerDefaultCodec(RuntimeException.class, new ExceptionCodec());
                MessageConsumer<BatchWrapper<K, V>> consumer = eventBus.consumer(PC_VERTX_BATCH_RECORD_LIST);
                consumer.handler(this::handle);
            }

            private void handle(Message<BatchWrapper<K, V>> batchWrapperEvent) {
                addInstanceMDC();
                log.debug("Vertical handling {}", batchWrapperEvent);
                BatchWrapper<K, V> recordList = batchWrapperEvent.body();
                List<WorkContainer<K, V>> batch = recordList.getBatch();
                List<Tuple<ConsumerRecord<K, V>, ?>> result = null;
                try {
                    result = runUserFunction(userFunction,
                            recordEntry -> {
                                log.debug("Callback for {}", recordEntry);
                            },
                            batch);
                } catch (Exception e) {
                    String msg = msg("Vertx level error handling running user function: {}", e);
                    log.error(msg, e);
                    batchWrapperEvent.fail(0, msg + Arrays.toString(e.getStackTrace()));
                }

                // encode results?
//                batchWrapperEvent.reply("it worked");


                ShardManager<K, V> sm = wm.getSm();
                List<Future> collect = batchWrapperEvent.body().getBatch().stream().map(x -> {
                    Future extensionPayload = (Future) x.getExtensionPayload();
                    return extensionPayload;
                }).collect(Collectors.toList());

                CompositeFuture composite = CompositeFuture.all(collect);
                composite.onFailure(throwable -> {
                    // handle failure of any of the user functions
                    log.error("failed", throwable);
                    RuntimeException error = new RuntimeException(throwable);
                    batchWrapperEvent.reply(error);
                    log.error("failed", throwable);
                });

//                composite.onComplete(event -> {
//                    log.error("complete {}", event.succeeded());
//                    boolean succeeded1 = event.succeeded();
//                    boolean failed1 = event.failed();
//                    Throwable cause = event.cause();
//                    CompositeFuture compositeFuture = event.result();
//                    List<Throwable> causes1 = compositeFuture.causes();
//                    List<Object> list = compositeFuture.list();
//                    CompositeFuture result1 = compositeFuture.result();
//                    boolean succeeded = compositeFuture.succeeded();
//                    int size = compositeFuture.size();
//                    boolean failed = event.failed();
//                    log.error("complete {}", event.succeeded());
//                });

//                batchWrapperEvent.reply(wcs);

//                batchWrapperEvent.reply(result);

                batchWrapperEvent.reply(batchWrapperEvent.body());
            }

            @Override
            public void start(Promise<Void> startPromise) throws Exception {

            }

            @Override
            public void stop(Promise<Void> stopPromise) throws Exception {

            }
        }, options);
    }

    /**
     * Sends the message to the Vert.x PC Vertical for processing
     */
    @Override
    protected void submitWorkToPoolInner(final Function<PollContextInternal<K, V>, List<?>> usersFunction,
                                         final Consumer<?> callback,
                                         final List<WorkContainer<K, V>> batch) {
        // convert from Vert.x Future to Java Future
        Promise<Object> promise = Promise.promise();
        Context context = vertx.getOrCreateContext();
        CompletableFuture<Object> objectCompletableFuture = new CompletableFuture<>();
        objectCompletableFuture.handleAsync((unused, throwable) -> {
            context.runOnContext(unused1 -> {
                if (throwable == null) {
                    promise.handle(Future.succeededFuture());
                } else {
                    promise.handle(Future.failedFuture(throwable));
                }
            });
            return null;
        });

        // isn't really used by vertx, as it only represents successful submission of the job to the vertical
        setWorkersFuture(batch, objectCompletableFuture);

        // send the records
        EventBus bus = vertx.eventBus();
        DeliveryOptions options = new DeliveryOptions();
        options.setSendTimeout(ofMinutes(5).toMillis());

        bus.request(PC_VERTX_BATCH_RECORD_LIST, new BatchWrapper<>(batch), options, event -> {
            // reply
            Message<Object> result = event.result();
            Object body = result.body();
            log.debug("Response from PC Vertical: {}", body);
            boolean failed = event.failed();
            Throwable cause = event.cause();
            if (cause != null) {
                RuntimeException error = new RuntimeException("Error response from Vertical", cause);
                log.error(error.getMessage());
                super.failureReason = error;
            }
            promise.complete();
        });
    }

    /**
     * The vert.x module doesn't use any thread pool for dispatching work, as the work is all done by the vert.x engine.
     * This thread is only used to dispatch the work to vert.x.
     */
    // TODO optimise thread usage by not using any extra thread here at all - go straight from the control thread to vert.x. - submit to the vertical isntead?
    @Override
    protected ThreadPoolExecutor setupWorkerPool(int poolSize) {
        return super.setupWorkerPool(1);
    }

    @Override
    public void vertxHttpReqInfo(Function<PollContext<K, V>, RequestInfo> requestInfoFunction,
                                 Consumer<Future<HttpResponse<Buffer>>> onSend,
                                 Consumer<AsyncResult<HttpResponse<Buffer>>> onWebRequestComplete) {
        vertxHttpRequest((WebClient webClient, PollContext<K, V> rec) -> {
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
    public void vertxHttpRequest(BiFunction<WebClient, PollContext<K, V>, HttpRequest<Buffer>> webClientRequestFunction,
                                 Consumer<Future<HttpResponse<Buffer>>> onSend,
                                 Consumer<AsyncResult<HttpResponse<Buffer>>> onWebRequestComplete) {

        vertxHttpWebClient((webClient, record) -> {
            HttpRequest<Buffer> call = carefullyRun(webClientRequestFunction, webClient, record);

            Future<HttpResponse<Buffer>> send = call.send(); // dispatches the work to vertx

            // hook in the users' call back for when the web request gets a response
            send.onComplete(onWebRequestComplete::accept);

            return send;
        }, onSend);
    }

    @Override
    public void vertxHttpWebClient(BiFunction<WebClient, PollContext<K, V>, Future<HttpResponse<Buffer>>> webClientRequestFunction,
                                   Consumer<Future<HttpResponse<Buffer>>> onWebRequestSentCallback) {

        // wrap single record function in batch function
        Function<PollContextInternal<K, V>, List<?>> userFuncWrapper = (context) -> {
            log.trace("Consumed a record ({}), executing void function...", context);

            Future<HttpResponse<Buffer>> futureWebResponse = carefullyRun(webClientRequestFunction, webClient, context.getPollContext());

            // execute user's onSend callback
            onWebRequestSentCallback.accept(futureWebResponse);

            addVertxHooks(context, futureWebResponse);

            return UniLists.of(futureWebResponse);
        };

        Consumer<Future<HttpResponse<Buffer>>> noOp = (ignore) -> {
        }; // don't need it, we attach to vertx futures for callback

        super.supervisorLoop(userFuncWrapper, noOp);
    }

    private void addVertxHooks(final PollContextInternal<K, V> context, final Future<?> send) {
        context.streamWorkContainers().forEach(wc -> {
            // attach internal handler
            wc.setWorkType(VERTX_TYPE);
            wc.setExtensionPayload(send);

            send.onSuccess(h -> {
                log.debug("Vert.x Vertical success");
                wc.onUserFunctionSuccess();
                addToMailbox(wc);
            });
            send.onFailure(h -> {
                log.error("Vert.x Vertical fail: {}", h.getMessage());
                wc.onUserFunctionFailure(h);
                addToMailbox(wc);
            });

            // add plugin callback hook
            send.onComplete(ar -> {
                log.trace("Running plugin hook");
                this.onVertxCompleteHook.ifPresent(Runnable::run);
            });
        });
    }

    @Override
    public void vertxFuture(final Function<PollContext<K, V>, Future<?>> result) {

        // wrap single record function in batch function
        Function<PollContextInternal<K, V>, List<?>> userFuncWrapper = context -> {
            log.trace("Consumed a record ({}), executing void function...", context);

            Future<?> send = carefullyRun(result, context.getPollContext());

            addVertxHooks(context, send);

            return UniLists.of(send);
        };

        Consumer<Future<?>> noOp = ignore -> {
        }; // don't need it, we attach to vertx futures for callback

        super.supervisorLoop(userFuncWrapper, noOp);
    }

    @Override
    public void batchVertxFuture(final Function<PollContext<K, V>, Future<?>> result) {

        Function<PollContextInternal<K, V>, List<?>> userFuncWrapper = context -> {

            Future<?> send = carefullyRun(result, context.getPollContext());

            addVertxHooks(context, send);

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
