package io.confluent.parallelconsumer.examples.reactor;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.confluent.parallelconsumer.reactor.ReactorProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.RetrySpec;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
public class ParallelServiceRouter {

    private final WireMockServer wireMock;
    Duration TIMEOUT = Duration.ofMinutes(1);
    ReactorProcessor<String, String> pc;
    HttpClient client = HttpClient.newHttpClient();
    HttpResponse.BodyHandler<String> responseBodyHandler = HttpResponse.BodyHandlers.ofString();
    int NUMBER_OF_ENDPOINTS_TO_SIMULATE = 2;
    Producer<String, String> producer;

    public ParallelServiceRouter(ReactorProcessor<String, String> pc, WireMockServer wireMockServer) {
        this.pc = pc;
        this.wireMock = wireMockServer;

        pc.subscribe(of(ReactorApp.inputTopic));

        pc.react(rec -> {
            List<String> endpoints = getEndPointsForRecord(rec);
            List<HttpRequest> requests = buildRequests(endpoints);

            Mono<List<HttpResponse<String>>> results = scatterGather(requests);
            results.subscribe(allResults -> {
                log.info("Subscribe all count: {}: {}", allResults.size(), allResults);
            });
            return results;
        });
    }

    private Mono<List<HttpResponse<String>>> scatterGather(List<HttpRequest> requests) {
        Flux<HttpResponse<String>> httpResponseFlux = sendRequests(requests);
        return gatherResults(httpResponseFlux);
    }

    private Mono<List<HttpResponse<String>>> gatherResults(Flux<HttpResponse<String>> httpResponseFlux) {
        return httpResponseFlux.collectList();
    }

    private Flux<HttpResponse<String>> sendRequests(List<HttpRequest> requests) {
        return Flux.fromIterable(requests)
                .flatMap(request -> {
                    log.info("Sending: {}", request);
                    CompletableFuture<HttpResponse<String>> future = client.sendAsync(request, responseBodyHandler);
                    future.whenComplete((stringHttpResponse, throwable) -> {
                        processComplete(request, stringHttpResponse, throwable);
                    });

                    Mono<HttpResponse<String>> httpResponseMono = Mono.fromFuture(future);
                    return httpResponseMono;
                })
                .doOnEach(httpResponseSignal ->
                        log.info("Response received: {}", httpResponseSignal));
    }

    /**
     * Either:
     *
     * retry locally,
     *
     * throw exception to have PC retry whole record,
     *
     * skip and log,
     *
     * or publish to a DLQ for handling elsewhere, with metadata attached
     */
    private void processComplete(HttpRequest request, HttpResponse<String> stringHttpResponse, Throwable throwable) {
        log.info("When complete {}.{}", stringHttpResponse, throwable);
        boolean failed = false;
        if (failed) {
            // build DLQ record with whatever metadata you need for later retry
            ProducerRecord<String, String> dlq = new ProducerRecord<>("DLQ", request.toString(), stringHttpResponse.toString());
            producer.send(dlq);
        }
    }

    /**
     * Uses the simulated delay endpoint
     *
     * @see io.confluent.csid.utils.WireMockUtils
     */
    private List<HttpRequest> buildRequests(List<String> endpoints) {
        HttpRequest.Builder builder = HttpRequest.newBuilder();
        return endpoints.stream().map(x -> builder.GET()
                        .uri(URI.create(wireMock.baseUrl() + "/error/?endpoint=" + x))
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * Given a record, returns a list of filtered servers and end points that it should be routed to
     */
    private List<String> getEndPointsForRecord(ConsumerRecord<String, String> rec) {
//        String payload = "Offset " + rec.offset() + " value " + rec.value();
        String payload = "o " + rec.offset();
        return IntStream.range(0, NUMBER_OF_ENDPOINTS_TO_SIMULATE)
                .mapToObj(x -> {
                    String requestPath = "EP " + x + " " + payload;
                    return URLEncoder.encode(requestPath);
                })
                .collect(Collectors.toList());
    }

}
