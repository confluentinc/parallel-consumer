package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;
import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.mockito.Mockito;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class ModelUtils {

    @Getter
    private final PCModuleTestEnv module;

    public ModelUtils() {
        this(new PCModuleTestEnv());
    }

    public WorkContainer<String, String> createWorkFor(long offset) {
        ConsumerRecord<String, String> mockCr = Mockito.mock(ConsumerRecord.class);
        WorkContainer<String, String> workContainer = new WorkContainer<>(0, mockCr, module);
        Mockito.doReturn(offset).when(mockCr).offset();
        return workContainer;
    }

    public EpochAndRecordsMap<String, String> createFreshWork() {
        return new EpochAndRecordsMap<>(createConsumerRecords(), module.workManager().getPm());
    }

    public ConsumerRecords<String, String> createConsumerRecords() {
        return new ConsumerRecords<>(UniMaps.of(getPartition(), UniLists.of(
                createConsumerRecord(topic)
        )));
    }

    @Getter
    final String topic = "topic";

    @NonNull
    public TopicPartition getPartition() {
        return new TopicPartition(topic, 0);
    }

    @NonNull
    public List<TopicPartition> getPartitions() {
        return UniLists.of(new TopicPartition(topic, 0));
    }

    private long nextOffset = 0L;

    @NonNull
    private ConsumerRecord<String, String> createConsumerRecord(String topic) {
        var cr = new ConsumerRecord<>(topic, 0, nextOffset, "a-key", "a-value");
        nextOffset++;
        return cr;
    }

    public ProducerRecord<String, String> createProducerRecords() {
        return new ProducerRecord<>(topic, "a-key", "a-value");
    }

    final String groupId = "cg-1";

    public ConsumerGroupMetadata consumerGroupMeta() {
        return new ConsumerGroupMetadata(groupId);
    }

    public List<ProducerRecord<String, String>> createProducerRecords(String topicName, long numberToSend) {
        return createProducerRecords(topicName, numberToSend, "");
    }

    public List<ProducerRecord<String, String>> createProducerRecords(String topicName, long numberToSend, String prefix) {
        List<ProducerRecord<String, String>> recs = new ArrayList<>();
        for (int i = 0; i < numberToSend; i++) {
            String key = prefix + "key-" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, "value-" + i);
            recs.add(record);
        }
        return recs;
    }

}
