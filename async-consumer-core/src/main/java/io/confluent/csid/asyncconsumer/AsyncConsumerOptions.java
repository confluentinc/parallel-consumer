package io.confluent.csid.asyncconsumer;

import lombok.Builder;
import lombok.Getter;

@Builder
public class AsyncConsumerOptions {
    enum ProcessingOrder {
        UNORDERED, PARTITION, KEY
    }

    @Getter
    @Builder.Default
    private ProcessingOrder ordering = ProcessingOrder.UNORDERED;
}
