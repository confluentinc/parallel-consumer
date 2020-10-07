package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.confluent.parallelconsumer.ParallelConsumerImpl;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerImplTestBase;
import io.confluent.csid.utils.KafkaTestUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
@ExtendWith(VertxExtension.class)
public class VertxTest extends ParallelConsumerImplTestBase {

    StreamingParallelVertxConsumerImpl<String, String> vertxAsync;

    protected static WireMockServer stubServer;

    static final protected String stubResponse = "Good times.";

    VertxParallelConsumerImpl.RequestInfo getGoodHost() {
        return new VertxParallelConsumerImpl.RequestInfo("localhost", stubServer.port(), "/", UniMaps.of());
    }

    VertxParallelConsumerImpl.RequestInfo getBadHost() {
        return new VertxParallelConsumerImpl.RequestInfo("localhost", 1, "", UniMaps.of());
    }

    @BeforeAll
    static public void setupWireMock() {
        WireMockConfiguration options = wireMockConfig().dynamicPort();
        stubServer = new WireMockServer(options);
        stubServer.start();
        MappingBuilder mappingBuilder = get(urlPathEqualTo("/"))
                .willReturn(aResponse()
                        .withBody(stubResponse));
        stubServer.stubFor(
                mappingBuilder);
    }

    @Override
    protected ParallelConsumerImpl initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        VertxOptions vertxOptions = new VertxOptions();
        Vertx vertx = Vertx.vertx(vertxOptions);
        WebClient wc = WebClient.create(vertx);
        ParallelConsumerOptions build = ParallelConsumerOptions.builder().build();
        vertxAsync = new StreamingParallelVertxConsumerImpl<>(consumerSpy, producerSpy, vertx, wc, build);

        return vertxAsync;
    }

    @BeforeEach
    public void setupData() {
        super.primeFirstRecord();
    }

    @SneakyThrows
    @Test
    public void sanityTest(Vertx vertx, VertxTestContext tc) {
        WebClient client = WebClient.create(vertx);
        HttpRequest<Buffer> bufferHttpRequest = client.get(getGoodHost().getPort(), getGoodHost().getHost(), "");
        bufferHttpRequest.send(tc.succeeding(response -> tc.verify(() -> {
            log.debug("callback {}", response.bodyAsString());
            tc.completeNow();
        })));
    }

    @Test
    @SneakyThrows
    public void failingHttpCall() {
        var latch = new CountDownLatch(1);
        vertxAsync.addVertxOnCompleteHook(latch::countDown);

        var tupleStream =
                vertxAsync.vertxHttpReqInfoStream((ConsumerRecord<String, String> rec) -> {
                    VertxParallelConsumerImpl.RequestInfo badHost = getBadHost();
                    return badHost;
                });

        //
        awaitLatch(latch);

        //
        assertCommits(of(0));

        // check results are failures
        var res = getResults(tupleStream);
        assertThat(res).doesNotContainNull();
        assertThat(res).extracting(AsyncResult::failed).containsOnly(true);
        assertThat(res).flatExtracting(x ->
                Arrays.asList(x.cause().getMessage().split(" ")))
                .contains("Connection", "refused:");
    }

    @SneakyThrows
    @Test
    public void testVertxFunctionFail(Vertx vertx, VertxTestContext tc) {
        var latch = new CountDownLatch(1);
        vertxAsync.addVertxOnCompleteHook(latch::countDown);

        var futureStream =
                vertxAsync.vertxHttpReqInfoStream((rec) -> {
                    log.debug("Inner user function");
                    return getBadHost();
                });

        // wait
        awaitLatch(latch);

        // verify
        var collect = futureStream.map(StreamingParallelVertxConsumerImpl.VertxCPResult::getAsr).collect(Collectors.toList());
        assertThat(collect).hasSize(1);
        Future<HttpResponse<Buffer>> actual = collect.get(0).onComplete(x -> {
        });
        await().until(actual::isComplete);
        assertThat(actual).isNotNull();

        actual.onComplete(tc.failing(ar -> tc.verify(() -> {
            assertThat(ar).hasMessageContainingAll("Connection", "refused");
            tc.completeNow();
        })));

        Assertions.assertThat(vertxAsync.workRemaining()).isEqualTo(1); // two failed requests still in queue for retry
    }

    @Test
    public void testHttpMinimal() {
        var latch = new CountDownLatch(1);
        vertxAsync.addVertxOnCompleteHook(latch::countDown);

        var futureStream =
                vertxAsync.vertxHttpReqInfoStream((rec) -> {
                    log.debug("Inner user function");
                    VertxParallelConsumerImpl.RequestInfo goodHost = getGoodHost();
                    var params = UniMaps.of("randomParam", rec.value());
                    goodHost.setParams(params);

                    return goodHost;
                });

        // wait
        awaitLatch(latch);

        //
        waitForOneLoopCycle();

        // verify
        var res = getResults(futureStream);

        KafkaTestUtils.assertCommits(producerSpy, of(0, 1));

        // test results are successes
        assertThat(res).extracting(x -> x.result().statusCode()).containsOnly(200);
        assertThat(res).extracting(x -> x.result().bodyAsString()).contains(stubResponse);
    }

    @SneakyThrows
    @Test
    public void testHttp() {
        var latch = new CountDownLatch(1);
        vertxAsync.addVertxOnCompleteHook(latch::countDown);

        var futureStream =
                vertxAsync.vertxHttpRequestStream((webClient, rec) -> {
                    log.debug("Inner user function");
                    var data = rec.value();
                    VertxParallelConsumerImpl.RequestInfo reqInfo = getGoodHost();
                    var httpRequest = webClient.get(reqInfo.getPort(), reqInfo.getHost(), reqInfo.getContextPath());
                    httpRequest = httpRequest.addQueryParam("randomParam", data);

                    return httpRequest;
                });

        awaitLatch(latch);

        var res = getResults(futureStream);

        // test results are successes
        assertThat(res).hasSize(1).doesNotContainNull();
        assertThat(res).extracting(AsyncResult::cause).containsOnlyNulls();
        assertThat(res).extracting(x -> x.result().statusCode()).containsOnly(200);
        assertThat(res).extracting(x -> x.result().bodyAsString()).contains(stubResponse);
    }

    private List<AsyncResult<HttpResponse<Buffer>>> getResults(
            Stream<StreamingParallelVertxConsumerImpl.VertxCPResult<String, String>> futureStream) {
        var collect = futureStream.map(StreamingParallelVertxConsumerImpl.VertxCPResult::getAsr).collect(Collectors.toList());
        return blockingGetResults(collect);
    }

    @Test
    @Disabled
    public void handleHttpResponseCodes() {
        assertThat(true).isFalse();
    }

    @SneakyThrows
    private <T> List<AsyncResult<T>> blockingGetResults(List<Future<T>> collect) {
        List<AsyncResult<T>> list = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(collect.size());
        for (Future<T> httpResponseFuture : collect) {
            httpResponseFuture.onComplete(x -> {
                list.add(x);
                countDownLatch.countDown();
            });
        }
        boolean success = countDownLatch.await(defaultTimeoutSeconds, SECONDS);
        if (!success)
            throw new AssertionError("Timeout reached");
        return list;
    }

}
