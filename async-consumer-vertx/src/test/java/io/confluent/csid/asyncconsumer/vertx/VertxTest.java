package io.confluent.csid.asyncconsumer.vertx;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.confluent.csid.asyncconsumer.AsyncConsumer.Tuple;
import io.confluent.csid.asyncconsumer.AsyncConsumerTestBase;
import io.confluent.csid.asyncconsumer.vertx.VertxAsyncConsumer.RequestInfo;
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
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Arrays.asList;
import static java.util.List.of;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@Slf4j
@ExtendWith(VertxExtension.class)
public class VertxTest extends AsyncConsumerTestBase {

    VertxAsyncConsumer<MyKey, MyInput> vertxAsync;

    private static WireMockServer stubServer;

    RequestInfo getGoodHost() {
        return new RequestInfo("localhost", stubServer.port(), "/", Map.of());
    }

    RequestInfo getBadHost() {
        return new RequestInfo("localhost", 1, "", Map.of());
    }

    static final String stubResponse = "Good times.";

    @BeforeEach
    public void setupVertxTest() {
        VertxOptions vertxOptions = new VertxOptions();
        Vertx vertx = Vertx.vertx(vertxOptions);
        WebClient wc = WebClient.create(vertx);
        vertxAsync = new VertxAsyncConsumer<>(consumerSpy, producerSpy, vertx, wc);
        vertxAsync.setLongPollTimeout(ofMillis(100));
        vertxAsync.setTimeBetweenCommits(ofMillis(100));
    }

    @BeforeAll
    static public void setupWireMock() {
        WireMockConfiguration options = wireMockConfig().dynamicPort();
        stubServer = new WireMockServer(options);
        stubServer.start();
        stubServer.stubFor(
                get(urlPathEqualTo("/"))
                        .willReturn(aResponse()
                                .withBody(stubResponse)));
    }

    @SneakyThrows
    @Test
    @Tag("slow")
    public void sanityTest(Vertx vertx, VertxTestContext tc) {
        WebClient client = WebClient.create(vertx);
        HttpRequest<Buffer> bufferHttpRequest = client.get("captive.apple.com", "");
        bufferHttpRequest.send(tc.succeeding(response -> tc.verify(() -> {
            log.debug("callback {}", response.bodyAsString());
            tc.completeNow();
        })));
    }

    @Test
    @SneakyThrows
    public void failingHttpCall() {
        var reentrantLock = new CountDownLatch(2);
        var tupleStream =
                vertxAsync.vertxHttp((ConsumerRecord<MyKey, MyInput> rec) -> {
                    reentrantLock.countDown();
                    return getBadHost();
                });

        reentrantLock.await(defaultTimeoutSeconds, SECONDS);

        vertxAsync.waitForNoInFlight(defaultTimeout);

        vertxAsync.close();

        verify(producerSpy, times(0).description("Nothing should be committed"))
                .commitTransaction();

        // check results are failures
        var res = getResults(tupleStream);
        assertThat(res).doesNotContainNull();
        assertThat(res).extracting(x -> x.failed()).containsOnly(true);
        assertThat(res).flatExtracting(x ->
                asList(x.cause().getMessage().split(" ")))
                .contains("Connection", "refused:");
    }

    @Test
    public void testVertxFunctionFail(Vertx vertx, VertxTestContext tc) {
        var futureStream =
                vertxAsync.vertxHttp((ConsumerRecord<MyKey, MyInput> rec) -> {
                    log.debug("Inner user function");
                    return getBadHost();
                });

        // wait
        vertxAsync.close();

        // verify
        var collect = futureStream.map(x -> x.getRight()).collect(Collectors.toList());
        assertThat(collect).hasSize(2);
        Future<HttpResponse<Buffer>> actual = collect.get(0).onComplete(x -> {
        });
        await().until(actual::isComplete);
        assertThat(actual).isNotNull();

        actual.onComplete(tc.failing(ar -> tc.verify(() -> {
            assertThat(ar).hasMessageContainingAll("Connection", "refused");
            tc.completeNow();
        })));

        assertThat(vertxAsync.workRemaining()).isEqualTo(2); // two failed requests still in queue for retry
    }

    @Test
    public void testHttpMinimal() {
        var futureStream =
                vertxAsync.vertxHttp((ConsumerRecord<MyKey, MyInput> rec) -> {
                    log.debug("Inner user function");
                    RequestInfo goodHost = getGoodHost();
                    var params = Map.of("randomParam", rec.value().getData());
                    goodHost.setParams(params);
                    return goodHost;
                });

        // wait
        vertxAsync.close(defaultTimeout);

        // verify
        var res = getResults(futureStream);

        assertCommits(of(1));

        // test results are successes
        assertThat(res).extracting(x -> x.result().statusCode()).containsOnly(200);
        assertThat(res).extracting(x -> x.result().bodyAsString()).contains(stubResponse);
    }

    @Test
    public void testHttp() {
        var futureStream =
                vertxAsync.vertxHttp((WebClient c, ConsumerRecord<MyKey, MyInput> rec) -> {
                    log.debug("Inner user function");
                    var data = rec.value().getData();
                    RequestInfo reqInfo = getGoodHost();
                    var req = c.get(reqInfo.getPort(), reqInfo.getHost(), reqInfo.getContextPath());
                    req = req.addQueryParam("randomParam", data);
                    return req;
                });

        vertxAsync.close(ofSeconds(60));

        var res = getResults(futureStream);

        // test results are successes
        assertThat(res).hasSize(2).doesNotContainNull();
        assertThat(res).extracting(x -> x.result().statusCode()).containsOnly(200);
        assertThat(res).extracting(x -> x.result().bodyAsString()).contains(stubResponse);
    }

    private List<AsyncResult<HttpResponse<Buffer>>> getResults(
            Stream<Tuple<ConsumerRecord<MyKey, MyInput>, Future<HttpResponse<Buffer>>>> futureStream) {
        var collect = futureStream.map(Tuple::getRight)
                .collect(Collectors.toList());
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

    @Test
    @Disabled
    public void testGeneralVertical() {
        vertxAsync.vertx();
        String result = null;
        assertThat(result).isNotNull();
    }
}
