package io.confluent.parallelconsumer.integrationTests;

import com.google.common.truth.Truth;
import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

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
    @Test
    void brokerRestart() {
//        var recordsProduced = 10_000;
        var recordsProduced = 100;
//        var recordsProduced = 10;
        String topicName = setupTopic();
        getKcu().produceMessages(topicName, recordsProduced);

        //
        pc = getKcu().buildPc(ParallelConsumerOptions.ProcessingOrder.UNORDERED, ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC, 500);
        AtomicInteger processedCount = new AtomicInteger();
        pc.subscribe(topicName);
        pc.poll(recordContexts -> {
//            ThreadUtils.sleepSecondsLog(1);
            processedCount.incrementAndGet();
        });

        // make sure we've started consuming already before disconnecting the broker
        await().untilAtomic(processedCount, is(greaterThan(10)));
        log.debug("Consumed 100");

        //
        simulateBrokerUnreachable();

        //
        checkPCState();

        log.debug("Stay disconnected for a while...");
        // todo how long to test we can recover from?
        ThreadUtils.sleepSecondsLog(10);

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

}
