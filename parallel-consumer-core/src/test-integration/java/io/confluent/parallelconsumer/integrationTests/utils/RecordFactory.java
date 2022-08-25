
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests.utils;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public class RecordFactory {

    public List<ProducerRecord<String, String>> createRecords(String topicName, long numberToSend) {
        List<ProducerRecord<String, String>> recs = new ArrayList<>();
        for (int i = 0; i < numberToSend; i++) {
            String key = "key-" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, "value-" + i);
            recs.add(record);
        }
        return recs;
    }
}
