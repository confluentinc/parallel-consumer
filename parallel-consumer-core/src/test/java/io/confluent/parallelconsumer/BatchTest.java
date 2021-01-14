package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * Basic tests for batch processing functionality
 */
@Slf4j
public class BatchTest extends ParallelEoSStreamProcessorTestBase {

    @Test
    void batch() {
        int numRecs = 5;
        var recs = ktu.sendRecords(numRecs);
        List<List<ConsumerRecord<String, String>>> received = new ArrayList<>();
        int batchSize = 2;
        parallelConsumer.pollBatch(batchSize, (List<ConsumerRecord<String, String>> x) -> {
            log.info("Batch of messages: {}", x);
            received.add(x);
        });

        //
        waitAtMost(ofSeconds(1)).alias("expected number of batches")
                .untilAsserted(() -> assertThat(received).hasSize(3));

        assertThat(received)
                .as("batch size")
                .allSatisfy(x -> assertThat(x).hasSizeLessThanOrEqualTo(batchSize))
                .as("all messages processed")
                .flatExtracting(x -> x).hasSameElementsAs(recs);
    }

}
