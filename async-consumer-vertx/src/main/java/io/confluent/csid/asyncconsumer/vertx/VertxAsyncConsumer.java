package io.confluent.csid.asyncconsumer.vertx;

import io.confluent.csid.asyncconsumer.AsyncConsumer;
import io.confluent.csid.asyncconsumer.WorkContainer;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.spi.VerticleFactory;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
public class VertxAsyncConsumer<K, V> extends AsyncConsumer<K, V> {

    private static final String VERTX_TYPE = "vert.x type";

    private Vertx vertx;

    private WebClient webClient;

    public VertxAsyncConsumer(Consumer consumer, Producer producer) {
        this(consumer, producer, Vertx.vertx(), null);
    }

    public VertxAsyncConsumer(Consumer consumer, Producer producer, Vertx vertx, WebClient webClient) {
        super(consumer, producer);
        if (vertx == null)
            vertx = Vertx.vertx();
        this.vertx = vertx;
        if (webClient == null)
            webClient = WebClient.create(vertx);
        this.webClient = webClient;
    }

    public void vertx() {
        throw new RuntimeException();
    }


    @Override
    public void close() {
        close(defaultTimeout);
    }

    @Override
    public void close(Duration timeout) {
        log.info("vertx async consumer closing...");
        waitForNoInFlight(timeout);
        webClient.close();
        Future<Void> close = vertx.close();
        await().until(close::isComplete);
        super.close(timeout);
    }

    public Stream<Tuple<ConsumerRecord<K, V>, Future<HttpResponse<Buffer>>>> vertxHttp(
            Function<ConsumerRecord<K, V>, RequestInfo> requestInfoFunction) {
        return this.vertxHttp((WebClient c, ConsumerRecord<K, V> rec) -> {
            RequestInfo reqInf = requestInfoFunction.apply(rec);
            HttpRequest<Buffer> req = c.get(reqInf.getPort(), reqInf.getHost(), reqInf.getContextPath());
            Map<String, String> params = reqInf.getParams();
            for (var entry : params.entrySet()) {
                req = req.addQueryParam(entry.getKey(), entry.getValue());
            }
            return req;
        });
    }

    public Stream<Tuple<ConsumerRecord<K, V>, Future<HttpResponse<Buffer>>>> vertxHttp(
            BiFunction<WebClient, ConsumerRecord<K, V>, HttpRequest<Buffer>> webClientRequestFunction) {
        var tupleStream = this.asyncPollAndStream((record) -> {
            HttpRequest<Buffer> call = webClientRequestFunction.apply(webClient, record);

            Future<HttpResponse<Buffer>> send = call.send(); // dispatches the work to vertx

            // attach internal handler
            WorkContainer<K, V> wc = wm.getWorkContainerForRecord(record); // todo thread safe?
            wc.setWorkType(VERTX_TYPE);
            // WorkContainer<K, V> wc = new WorkContainer<>(record, VERTX_TYPE);
            send.onSuccess(h -> {
                log.debug("Vert.x Vertical success");
                log.trace("Response body: {}", h.bodyAsString());
                // todo stream http result
                wc.onUserFunctionSuccess();
                addToMailbox(wc);
            });
            send.onFailure(h -> {
                log.debug("Vert.x Vertical fail: {}", h.getMessage());
                wc.onUserFunctionFailure();
                addToMailbox(wc);
            });
            // stream future back to client
            return List.of(send);
        });
        return tupleStream;
    }

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
            log.info("Vertx creation function success, user's function success");
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

    private boolean isVertxWork(WorkContainer<K, V> wc) {
        return wc.getWorkType().equals(VERTX_TYPE);
    }

//    @Override
//    protected boolean isWorkComplete(WorkContainer<K, V> wc) {
//        if (isVertxWork(wc)) {
//
//        } else {
//            return super.isWorkComplete(wc);
//        }
//    }

}