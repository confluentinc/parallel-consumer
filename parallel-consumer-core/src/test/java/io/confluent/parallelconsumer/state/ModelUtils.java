package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.TimeUtils;
import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mockito.Mockito;

@UtilityClass
public class ModelUtils {

    public static WorkContainer<String, String> createWorkFor(long offset) {
        //noinspection unchecked
        ConsumerRecord<String, String> mockCr = Mockito.mock(ConsumerRecord.class);
        WorkContainer<String, String> workContainer = new WorkContainer<>(0, mockCr, null, TimeUtils.getClock());
        Mockito.doReturn(offset).when(mockCr).offset();
        return workContainer;
    }
}
