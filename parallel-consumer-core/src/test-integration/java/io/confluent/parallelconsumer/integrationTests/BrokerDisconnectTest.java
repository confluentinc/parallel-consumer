package io.confluent.parallelconsumer.integrationTests;

import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.hamcrest.Matchers;

import java.util.concurrent.atomic.AtomicInteger;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

/**
 * Exercises PC's reaction to the broker connection being lost of the broker being restarted
 */
@Slf4j
class BrokerDisconnectTest extends BrokerIntegrationTest<String, String> {

    @Test
    void brokerRestart() {
        var recordsProduced = 100000;
        setupTestData();
        ParallelEoSStreamProcessor<String, String> pc = setupPc();

        AtomicInteger processedCount = new AtomicInteger();
        pc.poll(recordContexts -> {
            log.debug(recordContexts.toString());
            processedCount.incrementAndGet();
        });

        //
        await().untilAtomic(processedCount, Matchers.is(Matchers.greaterThan(100)));

        //
        terminateBroker();

        //
        checkPCState();

        //
        resumeBroker();

        //
        checkPCState();

        //
        await().untilAtomic(processedCount, Matchers.is(Matchers.greaterThan(recordsProduced)));


    }

    private void resumeBroker() {

    }

    private void checkPCState() {
    }

    private void terminateBroker() {

    }

    private ParallelEoSStreamProcessor<String, String> setupPc() {
        return null;
    }

    private void setupTestData() {

    }
}