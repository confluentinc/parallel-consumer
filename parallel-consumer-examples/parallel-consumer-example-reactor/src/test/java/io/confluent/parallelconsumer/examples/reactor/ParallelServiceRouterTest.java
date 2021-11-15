package io.confluent.parallelconsumer.examples.reactor;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.LongPollingMockConsumer;
import io.confluent.csid.utils.WireMockUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.reactor.ReactorProcessor;
import io.confluent.parallelconsumer.truth.LongPollingMockConsumerSubject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;

import static io.confluent.parallelconsumer.examples.reactor.ReactorApp.inputTopic;
import static pl.tlinkowski.unij.api.UniLists.of;


@Timeout(20)
class ParallelServiceRouterTest {

    WireMockServer wireMockServer = new WireMockUtils().setupWireMock();
    int port = wireMockServer.port();

    @Test
    void sendParallelWebRequests() {
        ReactorAppTest.ReactorAppAppUnderTest app = new ReactorAppTest.ReactorAppAppUnderTest(port);
        LongPollingMockConsumer<String, String> mockConsumer = (LongPollingMockConsumer<String, String>)app.createKafkaConsumer();
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .consumer(mockConsumer)
                .producer(app.createKafkaProducer())
                .build();

        ReactorProcessor<String, String> pc = new ReactorProcessor<>(options);

        ParallelServiceRouter router = new ParallelServiceRouter(pc, wireMockServer);
        mockConsumer.subscribeWithRebalanceAndAssignment(of(inputTopic), 1);

        String s = "a key 1";
        String a_value = "a value";
        addRecord(mockConsumer, s, a_value, 0);
        addRecord(mockConsumer, "a key 2", a_value, 1);
        addRecord(mockConsumer, "a key 3", a_value, 2);

        Awaitility.await().pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            KafkaTestUtils.assertLastCommitIs(mockConsumer, 3);
        });
        mockConsumer.close();
    }

    private void addRecord(LongPollingMockConsumer<String, String> mc, String s, String a_value, int i) {
        mc.addRecord(new ConsumerRecord<>(inputTopic, 0, i, s, a_value));
    }

    static class ParallelReactorAppAppUnderTest extends ReactorAppTest.ReactorAppAppUnderTest {

        public ParallelReactorAppAppUnderTest(int port) {
            super(port);
        }

    }

}