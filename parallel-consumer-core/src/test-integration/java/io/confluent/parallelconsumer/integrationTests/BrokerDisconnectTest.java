package io.confluent.parallelconsumer.integrationTests;

import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.hamcrest.Matchers;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Exercises PC's reaction to the broker connection being lost of the broker being restarted
 */
@Slf4j
class BrokerDisconnectTest extends BrokerIntegrationTest<String, String> {

    @SneakyThrows
    @Test
    void brokerRestart() {
        var recordsProduced = 100000;
        String topicName = setupTopic();
        getKcu().produceMessages(topicName, recordsProduced);

        //
        ParallelEoSStreamProcessor<String, String> pc = getKcu().buildPc();
        AtomicInteger processedCount = new AtomicInteger();
        pc.subscribe(topicName);
        pc.poll(recordContexts -> {
            log.debug(recordContexts.toString());
            processedCount.incrementAndGet();
        });

        //
        await().atMost(Duration.ofSeconds(60)).untilAtomic(processedCount, Matchers.is(Matchers.greaterThan(100)));
        log.warn("Consumed 100");

        //
        terminateBroker();

        //
        checkPCState();

        //
        startNewBroker();

        //
        checkPCState();

        //
        await().atMost(Duration.ofSeconds(60)).untilAtomic(processedCount, Matchers.is(Matchers.greaterThan(recordsProduced)));


    }

    private void checkPCState() {
        log.debug("");
    }

}
