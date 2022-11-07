package io.confluent.parallelconsumer.integrationTests;

import com.google.common.truth.Truth;
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
    void brokerRestart() {
        var recordsProduced = 100000;
        String topicName = setupTopic();
        getKcu().produceMessages(topicName, recordsProduced);

        //
        pc = getKcu().buildPc(ParallelConsumerOptions.ProcessingOrder.UNORDERED, ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC, 500);
        AtomicInteger processedCount = new AtomicInteger();
        pc.subscribe(topicName);
        pc.poll(recordContexts -> {
            processedCount.incrementAndGet();
        });

        // make sure we've started consuming already before disconnecting the broker
        await().untilAtomic(processedCount, is(greaterThan(100)));
        log.debug("Consumed 100");

        //
        simulateBrokerUnreachable();

        //
        checkPCState();

        log.debug("Stay disconnected for a while...");
        Thread.sleep(15000);
        log.debug("Finished disconnect, resuming connection.");

        //
        simulateBrokerResume();

        //
        checkPCState();

        //
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> Truth.assertThat(processedCount.get()).isAtLeast(recordsProduced));
    }

    private void checkPCState() {
        // todo check state
        assertThat(pc).isNotClosedOrFailed();
    }

}
