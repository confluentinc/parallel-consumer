package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.WorkContainer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.ByteBuffer;

/**
 * Utility methods for Consumer Records
 */
public class ParallelConsumerRecord {

    /**
     * Get the number of failed attempts for a record.
     *
     * @param record the record
     * @return the number of failed attempts
     */
    // TODO consider making this information available through a read only ConsumerRecord wrapper that only public API info (as WorkContainer is very internal and writeabe)
    public static int getFailedCount(final ConsumerRecord<String, String> record) {
        Headers headers = record.headers();
        Header header = headers.lastHeader(WorkContainer.FAILED_COUNT_HEADER);
        if (header != null) {
            byte[] value = header.value();
            ByteBuffer wrap = ByteBuffer.wrap(value);
            return wrap.getInt();
        } else {
            return 0;
        }
    }

}
