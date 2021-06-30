
/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */
package io.confluent.parallelconsumer.vertx.integrationTests;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.RequestListener;
import com.github.tomakehurst.wiremock.http.Response;
import io.confluent.csid.utils.LatchTestUtils;
import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.csid.utils.StringUtils;
import io.confluent.csid.utils.TrimListRepresentation;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import io.confluent.parallelconsumer.vertx.VertxParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.awaitility.core.ConditionTimeoutException;
import org.eclipse.jetty.util.ConcurrentArrayQueue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static io.confluent.csid.utils.LatchTestUtils.awaitLatch;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static java.lang.Thread.getAllStackTraces;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.data.Percentage.withPercentage;
import static org.awaitility.Awaitility.waitAtMost;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * @see #testVertxConcurrency()
 */
@Testcontainers
@Slf4j
class VertxConcurrencyIT extends BrokerIntegrationTest {

    private static ProgressBar bar;
    static final int expectedMessageCount = 200;

    public List<String> consumedKeys = Collections.synchronizedList(new ArrayList<>());
    public AtomicInteger processedCount = new AtomicInteger(0);
    public AtomicInteger httpResponceReceivedCount = new AtomicInteger(0);

    public static WireMockServer stubServer;

    static CountDownLatch responseLock = new CountDownLatch(1);

    static Queue<Request> parallelRequests = new ConcurrentArrayQueue<>();

    VertxConcurrencyIT() {
        bar = ProgressBarUtils.getNewMessagesBar(log, expectedMessageCount);
    }

    @BeforeAll
    static void setupWireMock() {
        WireMockConfiguration options = wireMockConfig().dynamicPort()
                .containerThreads(expectedMessageCount * 2); // ensure wiremock has enough threads to respond to everything in parallel

        stubServer = new WireMockServer(options);
        MappingBuilder mappingBuilder = get(urlPathEqualTo("/"))
                .willReturn(aResponse());

        stubServer.stubFor(mappingBuilder);

        stubServer.addMockServiceRequestListener(new RequestListener() {

            @Override
            public void requestReceived(final Request request, final Response response) {
                log.debug("req: {}", request);
                parallelRequests.add(request);
                bar.stepBy(1);
                awaitLatch(responseLock, 30); // latch timeout should be longer than awaitility's
                log.trace("unlocked");
            }
        });

        stubServer.start();
    }

    @AfterAll
    static void close() {
        stubServer.stop();
    }

    /**
     * This test uses a wire mock server which blocks responding to all requests, until it has received a certain number
     * of requests in parallel. Once this count has been reached, the global latch is released, and all requests are
     * responded to.
     * <p>
     * This is used to sanity test that the PC vertx module is indeed sending the number of concurrent requests that we
     * would expect.
     */
    @Test
    @SneakyThrows
    void testVertxConcurrency() {
        var commitMode = PERIODIC_CONSUMER_ASYNCHRONOUS;
        var order = ParallelConsumerOptions.ProcessingOrder.UNORDERED;

        String inputName = setupTopic(this.getClass().getSimpleName() + "-input-" + RandomUtils.nextInt());

        // pre-produce messages to input-topic
        List<String> expectedKeys = new ArrayList<>();


        log.info("Producing {} messages before starting test", expectedMessageCount);
        List<Future<RecordMetadata>> sends = new ArrayList<>();
        try (Producer<String, String> kafkaProducer = kcu.createNewProducer(false)) {
            for (int i = 0; i < expectedMessageCount; i++) {
                String key = "key-" + i;
                Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<>(inputName, key, "value-" + i), (meta, exception) -> {
                    if (exception != null) {
                        log.error("Error sending, ", exception);
                    }
                });
                sends.add(send);
                expectedKeys.add(key);
            }
            log.debug("Finished sending test data");
        }
        // make sure we finish sending before next stage
        log.debug("Waiting for broker acks");
        for (Future<RecordMetadata> send : sends) {
            send.get();
        }
        assertThat(sends).hasSize(expectedMessageCount);

        // run parallel-consumer
        log.debug("Starting test");
        KafkaProducer<String, String> newProducer = kcu.createNewProducer(commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER));

        Properties consumerProps = new Properties();
        KafkaConsumer<String, String> newConsumer = kcu.createNewConsumer(true, consumerProps);

        var pc = new VertxParallelEoSStreamProcessor<String, String>(ParallelConsumerOptions.<String, String>builder()
                .ordering(order)
                .consumer(newConsumer)
                .producer(newProducer)
                .commitMode(commitMode)
                .maxConcurrency(1000)
                .build());
        pc.subscribe(of(inputName));

        // sanity
        TopicPartition tp = new TopicPartition(inputName, 0);
        Map<TopicPartition, Long> beginOffsets = newConsumer.beginningOffsets(of(tp));
        Map<TopicPartition, Long> endOffsets = newConsumer.endOffsets(of(tp));
        assertThat(endOffsets.get(tp)).isEqualTo(expectedMessageCount);
        assertThat(beginOffsets.get(tp)).isEqualTo(0L);


        pc.vertxHttpReqInfo(record -> {
                    consumedKeys.add(record.key());
                    return new VertxParallelEoSStreamProcessor.RequestInfo("localhost", stubServer.port(), "/", UniMaps.of());
                }, onSend -> {
                    processedCount.incrementAndGet();
                }, onWebResponseAsyncResult -> {
                    httpResponceReceivedCount.incrementAndGet();
                    log.trace("Response received complete {}", onWebResponseAsyncResult);
                }
        );

        // wait for all pre-produced messages to be processed and produced
        log.info("Waiting for {} requests in parallel on server.", expectedMessageCount);
        Assertions.useRepresentation(new TrimListRepresentation());
        var failureMessage = StringUtils.msg("Mock server receives {} requests in parallel from vertx engine",
                expectedMessageCount);
        try {
            waitAtMost(ofSeconds(10))
                    .pollInterval(ofSeconds(1))
                    .alias(failureMessage)
                    .untilAsserted(() -> {
                        log.info("got {}/{}", parallelRequests.size(), expectedMessageCount);
                        assertThat(parallelRequests.size()).isEqualTo(expectedMessageCount);
                    });
        } catch (ConditionTimeoutException e) {
            fail(failureMessage + "\n" + e.getMessage());
        }
        log.info("All {} requests received in parallel by server, releasing server response lock.", expectedMessageCount);

        // all requests were received in parallel, so unlock the server to respond to all of them
        LatchTestUtils.release(responseLock);

        assertNumberOfThreads();

        log.info("Waiting for {} responses from server.", expectedMessageCount);
        waitAtMost(ofSeconds(10))
                .alias(failureMessage)
                .untilAsserted(() -> {
                    assertThat(httpResponceReceivedCount).hasValue(expectedMessageCount);
                });

        log.info("All {} responses received.", expectedMessageCount);

        assertNumberOfThreads();

        // close
        bar.close();
        pc.closeDrainFirst();

        // sanity
        assertThat(expectedMessageCount).isEqualTo(processedCount.get());
        assertThat(responseLock.getCount()).isZero();
    }

    /**
     * Should be around this number of threads running - introduced to sanity check thread count looks right
     */
    private void assertNumberOfThreads() {
        Set<Thread> threadKeys = getAllStackTraces().keySet();
        String pcPrefix = "pc-";
        String wireMockPrefix = "qtp";
        long pcThreadCount = threadKeys.stream().filter(x -> x.getName().startsWith(pcPrefix)).count();
        long wireMockThreadCount = threadKeys.stream().filter(x -> x.getName().startsWith(wireMockPrefix)).count();

        int expectedPCThreads = 3;

        if (pcThreadCount > 0) // pc may not have started
        {
            log.info("Checking there are only {} PC threads running", expectedPCThreads);
            assertThat(pcThreadCount)
                    .as("Number of Parallel Consumer threads outside expected estimates")
                    .isEqualTo(expectedPCThreads);
        }

        log.info("Checking there are about ~{} wire mock threads running to process requests in parallel from vert.x", expectedMessageCount);
        assertThat(wireMockThreadCount)
                .as("Number of wiremock threads outside expected estimates")
                .isCloseTo(expectedMessageCount, withPercentage(8));

    }

}
