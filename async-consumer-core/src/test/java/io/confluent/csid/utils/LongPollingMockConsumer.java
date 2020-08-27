package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;

/**
 * Used in tests to stub out the behaviour of the real Broker and Client's long polling system (the mock Kafka Consumer
 * doesn't have this behaviour).
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class LongPollingMockConsumer<K, V> extends MockConsumer<K, V> {

    public LongPollingMockConsumer(OffsetResetStrategy offsetResetStrategy) {
        super(offsetResetStrategy);
    }

    @Override
    public synchronized ConsumerRecords<K, V> poll(Duration timeout) {
        var records = super.poll(timeout);

        if (records.isEmpty()) {
            log.debug("No records returned, simulating long poll with sleep for requested long poll timeout of {}...", timeout);
            try {
                synchronized (this) {
                    this.wait(timeout.toMillis());
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted", e);
            }
            log.debug("Simulated long poll of ({}) finished.", timeout);
        } else {
            log.debug("Polled and found {} records...", records.count());
        }
        return records;
    }

    @Override
    public synchronized void wakeup() {
        log.debug("Interrupting mock long poll...");
        synchronized (this) {
            this.notifyAll();
        }
    }


}
