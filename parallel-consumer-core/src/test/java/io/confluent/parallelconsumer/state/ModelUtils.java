package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;

@RequiredArgsConstructor
public class ModelUtils {

    private final PCModuleTestEnv module;

    public WorkContainer<String, String> createWorkFor(long offset) {
        ConsumerRecord<String, String> mockCr = Mockito.mock(ConsumerRecord.class);
        WorkContainer<String, String> workContainer = new WorkContainer<>(0, mockCr, mock(PCModule.class));
        Mockito.doReturn(offset).when(mockCr).offset();
        return workContainer;
    }

}
