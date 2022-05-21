package io.confluent.parallelconsumer.integrationTests;

import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class BrokerDisconnectTest extends BrokerIntegrationTest<String, String> {

    @Test
    void brokerRestart() {
        setupTestData();
        ParallelEoSStreamProcessor<String, String> pc = setupPc();

//        KafkaConsumer<String, String> newConsumer = kcu.createNewConsumer();
//        //
//        boolean tx = true;
//        ParallelConsumerOptions<String, String> options = ParallelConsumerOptions.<String, String>builder()
//                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
//                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
//                .producer(kcu.createNewProducer(tx))
//                .consumer(newConsumer)
//                .maxConcurrency(3)
//                .build();
//
//        ParallelEoSStreamProcessor<String, String> async = new ParallelEoSStreamProcessor<>(options);
//        async.subscribe(Pattern.compile(topic));

        AtomicInteger processedCount = new AtomicInteger();
        pc.poll(recordContexts -> {
            log.debug(recordContexts.toString());
            processedCount.incrementAndGet();
        });

        Awaitility.await().untilAtomic(processedCount, )

    }

    private ParallelEoSStreamProcessor<String, String> setupPc() {
        return null;
    }

    private void setupTestData() {

    }
}
