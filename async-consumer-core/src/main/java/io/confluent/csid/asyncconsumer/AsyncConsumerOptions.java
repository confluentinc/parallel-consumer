package io.confluent.csid.asyncconsumer;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.ProducerRecord;

@Builder
public class AsyncConsumerOptions {
    enum ProcessingOrder {
        NONE, PARTITION, KEY
    }

    @Getter
    @Builder.Default
    private ProcessingOrder ordering = ProcessingOrder.NONE;
}
