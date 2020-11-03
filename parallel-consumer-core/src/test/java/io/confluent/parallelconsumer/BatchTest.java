package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

@Slf4j
public class BatchTest {

    @Test
    void batch(){
        try(var p = new ParallelEoSStreamProcessor<String, String>(null, null, null);){
            p.pollBatch(5, (List<ConsumerRecord<String, String>> x) -> {
                log.info("Batch of messages: {}", x);
            });
        }
    }
}
