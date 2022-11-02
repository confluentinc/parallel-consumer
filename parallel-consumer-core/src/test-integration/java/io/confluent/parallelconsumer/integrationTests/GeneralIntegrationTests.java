package io.confluent.parallelconsumer.integrationTests;

import org.junit.jupiter.api.Test;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;

public class GeneralIntegrationTests extends BrokerIntegrationTest<String, String> {
    @Test
    void lessKeysThanThreads() {
        ensureTopic("lessKeysThanThreads", 5);

        super.produceMessages(10_000);
        var pc = getKcu().buildPc(KEY);
        pc.poll();

    }
}
