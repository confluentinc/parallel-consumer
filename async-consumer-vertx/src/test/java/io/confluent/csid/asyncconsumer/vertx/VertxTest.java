package io.confluent.csid.asyncconsumer.vertx;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.confluent.csid.asyncconsumer.AsyncConsumer;
import io.confluent.csid.asyncconsumer.AsyncConsumerOptions;
import io.confluent.csid.asyncconsumer.AsyncConsumerTestBase;
import io.confluent.csid.asyncconsumer.vertx.StreamingAsyncVertxConsumer.VertxCPResult;
import io.confluent.csid.asyncconsumer.vertx.VertxAsyncConsumer.RequestInfo;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@Slf4j
@ExtendWith(VertxExtension.class)
public class VertxTest extends AsyncConsumerTestBase {

    StreamingAsyncVertxConsumer<String, String> vertxAsync;

    protected static WireMockServer stubServer;

    static final protected String stubResponse = "Good times.";

    RequestInfo getGoodHost() {
        return new RequestInfo("localhost", stubServer.port(), "/", UniMaps.of());
    }

    RequestInfo getBadHost() {
        return new RequestInfo("localhost", 1, "", UniMaps.of());
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
    protected AsyncConsumer initAsyncConsumer(AsyncConsumerOptions asyncConsumerOptions) {
        VertxOptions vertxOptions = new VertxOptions();
        Vertx vertx = Vertx.vertx(vertxOptions);
        WebClient wc = WebClient.create(vertx);
        AsyncConsumerOptions build = AsyncConsumerOptions.builder().build();
        vertxAsync = new StreamingAsyncVertxConsumer<>(consumerSpy, producerSpy, vertx, wc, build);

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
                    RequestInfo badHost = getBadHost();
                    return badHost;
                });

        //
        awaitLatch(latch);

        //
        verify(producerSpy, times(0).description("Nothing should be committed"))
                .commitTransaction();

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
        var collect = futureStream.map(VertxCPResult::getAsr).collect(Collectors.toList());
        assertThat(collect).hasSize(1);
        Future<HttpResponse<Buffer>> actual = collect.get(0).onComplete(x -> {
        });
        await().until(actual::isComplete);
        assertThat(actual).isNotNull();

        actual.onComplete(tc.failing(ar -> tc.verify(() -> {
            assertThat(ar).hasMessageContainingAll("Connection", "refused");
            tc.completeNow();
        })));

        assertThat(vertxAsync.workRemaining()).isEqualTo(1); // two failed requests still in queue for retry
    }

    @Test
    public void testHttpMinimal() {
        var latch = new CountDownLatch(1);
        vertxAsync.addVertxOnCompleteHook(latch::countDown);

        var futureStream =
                vertxAsync.vertxHttpReqInfoStream((rec) -> {
                    log.debug("Inner user function");
                    RequestInfo goodHost = getGoodHost();
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

        KafkaTestUtils.assertCommits(producerSpy, UniLists.of(0));

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
                    RequestInfo reqInfo = getGoodHost();
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
            Stream<VertxCPResult<String, String>> futureStream) {
        var collect = futureStream.map(VertxCPResult::getAsr).collect(Collectors.toList());
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
