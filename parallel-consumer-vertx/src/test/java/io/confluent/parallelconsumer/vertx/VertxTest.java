package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import com.github.tomakehurst.wiremock.WireMockServer;
import io.confluent.csid.utils.WireMockUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.vertx.VertxParallelEoSStreamProcessor.RequestInfo;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Isolated;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.csid.utils.LatchTestUtils.awaitLatch;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static pl.tlinkowski.unij.api.UniLists.of;

@Isolated
@Slf4j
@ExtendWith(VertxExtension.class)
class VertxTest extends ParallelEoSStreamProcessorTestBase {

    JStreamVertxParallelEoSStreamProcessor<String, String> vertxAsync;

    WireMockServer stubServer;

    RequestInfo getGoodHost() {
        return new RequestInfo("localhost", stubServer.port(), "/", UniMaps.of());
    }

    RequestInfo getBadRequest() {
        int badPort = 1;
        String badHostname = "xxxxxxxxx"; // bad host names seem to fail faster than valid host names with invalid ports
        return new RequestInfo(badHostname, badPort, "/", UniMaps.of());
    }

    @BeforeEach
    void setupWireMock() {
        WireMockUtils wireMockUtils = new WireMockUtils();
        stubServer = wireMockUtils.setupWireMock();
    }

    @AfterEach
    void closeWireMock() {
        stubServer.stop();
    }

    @Override
    protected AbstractParallelEoSStreamProcessor initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        VertxOptions vertxOptions = new VertxOptions();
        Vertx vertx = Vertx.vertx(vertxOptions);
        WebClient wc = WebClient.create(vertx);
        var build = parallelConsumerOptions.toBuilder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER) // force tx
                .maxConcurrency(10)
                .build();
        vertxAsync = new JStreamVertxParallelEoSStreamProcessor<>(vertx, wc, build);

        return vertxAsync;
    }

    @BeforeEach
    public void setupData() {
        super.primeFirstRecord();
    }

    @SneakyThrows
    @Test
    void sanityTest(Vertx vertx, VertxTestContext tc) {
        WebClient client = WebClient.create(vertx);
        HttpRequest<Buffer> bufferHttpRequest = client.get(getGoodHost().getPort(), getGoodHost().getHost(), "");
        bufferHttpRequest.send(tc.succeeding(response -> tc.verify(() -> {
            log.debug("callback {}", response.bodyAsString());
            tc.completeNow();
        })));
    }

    @Test
    void failingHttpCall() {
        var latch = new CountDownLatch(1);
        vertxAsync.addVertxOnCompleteHook(latch::countDown);

        var tupleStream =
                vertxAsync.vertxHttpReqInfoStream((ConsumerRecord<String, String> rec) -> getBadRequest());

        //
        awaitLatch(latch);

        //
        assertCommits(of());

        // check results are failures
        var res = getResults(tupleStream);
        assertThat(res).doesNotContainNull();
        assertThat(res).extracting(AsyncResult::failed).containsOnly(true);
        assertThat(res).flatExtracting(x ->
                Arrays.asList(x.cause().getMessage().split(" ")))
                .contains("failed", "resolve");
    }

    // todo how is this different from #failingHttpCall ?
    @SneakyThrows
    @Test
    void testVertxFunctionFail(Vertx vertx, VertxTestContext tc) {
        var latch = new CountDownLatch(1);
        vertxAsync.addVertxOnCompleteHook(latch::countDown);

        var futureStream =
                vertxAsync.vertxHttpReqInfoStream((rec) -> {
                    log.debug("Inner user function");
                    return getBadRequest();
                });

        // wait
        awaitLatch(latch);

        // verify
        var collect = futureStream.map(JStreamVertxParallelEoSStreamProcessor.VertxCPResult::getAsr).collect(Collectors.toList());
        assertThat(collect).hasSize(1);
        Future<HttpResponse<Buffer>> actual = collect.get(0).onComplete(x -> {
        });
        await().until(actual::isComplete);
        assertThat(actual).isNotNull();

        actual.onComplete(tc.failing(ar -> tc.verify(() -> {
            assertThat(ar).hasMessageContainingAll("failed", "resolve");
            tc.completeNow();
        })));

        Assertions.assertThat(vertxAsync.workRemaining()).isEqualTo(1); // two failed requests still in queue for retry
    }

    @Test
    void testHttpMinimal() {
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

        ktu.assertCommits(producerSpy, of(1));

        // test results are successes
        assertThat(res).extracting(x -> x.result().statusCode()).containsOnly(200);
        assertThat(res).extracting(x -> x.result().bodyAsString()).contains(WireMockUtils.stubResponse);
    }

    @SneakyThrows
    @Test
    void testHttp() {
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
        assertThat(res).extracting(x -> x.result().bodyAsString()).contains(WireMockUtils.stubResponse);
    }

    private List<AsyncResult<HttpResponse<Buffer>>> getResults(
            Stream<JStreamVertxParallelEoSStreamProcessor.VertxCPResult<String, String>> futureStream) {
        var collect = futureStream.map(JStreamVertxParallelEoSStreamProcessor.VertxCPResult::getAsr).collect(Collectors.toList());
        return blockingGetResults(collect);
    }

    @Test
    @Disabled
    void handleHttpResponseCodes() {
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

    @SneakyThrows
    @Test
    void genericVertxFuture(Vertx vertx, VertxTestContext tc) {
        primeFirstRecord();
        primeFirstRecord();

        var latch = new CountDownLatch(1);
        vertxAsync.addVertxOnCompleteHook(latch::countDown);

        var latchTwo = new CountDownLatch(1);

        Checkpoint cp = tc.checkpoint(3);

        vertxAsync.vertxFuture(rec -> vertx.executeBlocking(event -> {
            log.debug("Inner user function {}", rec);
            var data = rec.value();

            try {
                log.info("Waiting");
                latchTwo.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            cp.flag();
            log.info("Finished waiting");

            event.complete();
        }));

        log.info("Pausing");
        Thread.sleep(1000L);
        latchTwo.countDown();
        log.info("Counted down");

        awaitLatch(latch);
        log.info("Latch gotten");
    }

}
