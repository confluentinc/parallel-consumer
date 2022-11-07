package io.confluent.parallelconsumer.integrationTests;

import com.google.common.truth.Truth;
import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.shaded.org.hamcrest.Matchers.greaterThan;
import static org.testcontainers.shaded.org.hamcrest.Matchers.is;

/**
 * Exercises PC's reaction to the broker connection being lost of the broker being restarted
 */
@Slf4j
class BrokerDisconnectTest extends BrokerIntegrationTest {

    //        int recordsProduced = 10_000;
    int recordsProduced = 100;
//        int recordsProduced = 10;

    AtomicInteger processedCount = new AtomicInteger();

    ParallelEoSStreamProcessor<String, String> pc;

    @SneakyThrows
    @Test
    void proxyTest() {
        simulateBrokerUnreachable();
        String topicName = setupTopic();
        getKcu().produceMessages(topicName, 1);
        Truth.assertThat(false).isTrue();
    }

    @SneakyThrows
    void setupAndWarmUp(KafkaClientUtils kcu) {
        String topicName = setupTopic();
        kcu.produceMessages(topicName, recordsProduced);

        //
        pc = kcu.buildPc(ParallelConsumerOptions.ProcessingOrder.UNORDERED, ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC, 500);
        pc.subscribe(topicName);
        pc.poll(recordContexts -> {
//            ThreadUtils.sleepSecondsLog(1);
            processedCount.incrementAndGet();
        });

        // make sure we've started consuming already before disconnecting the broker
        await().untilAtomic(processedCount, is(greaterThan(10)));
        log.debug("Consumed 100");
    }

    @SneakyThrows
    @Test
    void brokerRestart() {
        setupAndWarmUp(getKcu());

        //
        simulateBrokerUnreachable();

        //
        checkPCState();

        log.debug("Stay disconnected for a while...");
        Truth.assertThat(processedCount.get()).isLessThan(recordsProduced);
        // todo how long to test we can recover from?
        // 10 seconds, doesn't notice
        // 120 unknown error
        ThreadUtils.sleepSecondsLog(180);

        //
        checkPCState();

        //
        simulateBrokerResume();

        //
        checkPCState();

        //
        await()
                .atMost(Duration.ofMinutes(60))
                .failFast("pc has crashed", () -> pc.isClosedOrFailed())
                .untilAsserted(() -> Truth
                        .assertThat(processedCount.get())
                        .isAtLeast(recordsProduced));
    }

    private void checkPCState() {
        // todo check state
        assertThat(pc).isNotClosedOrFailed();
    }

    @SneakyThrows
    @Test
    void brokerShutdown() {
        // need ot start
        KafkaContainer finikyKafka = getKafkaContainer();

        KafkaClientUtils kafkaClientUtils = new KafkaClientUtils(finikyKafka, getToxiproxy());

        setupAndWarmUp(kafkaClientUtils);

        //
        brokerDies(finikyKafka);

        //
        checkPCState();

        // wait a while
        ThreadUtils.sleepSecondsLog(120);

        //
        checkPCState();

        //
        await()
                .atMost(Duration.ofMinutes(60))
                .failFast("pc has crashed", () -> pc.isClosedOrFailed())
                .untilAsserted(() -> Truth
                        .assertThat(processedCount.get())
                        .isAtLeast(recordsProduced));
    }

    private static void brokerDies(KafkaContainer finikyKafka) {
        log.debug("Making broker unavailable");
        finikyKafka.close();
    }

}
