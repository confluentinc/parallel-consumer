package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.JavaUtils;
import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;
import one.util.streamex.LongStreamEx;
import one.util.streamex.StreamEx;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.mockito.Mockito;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public class PolledTestBatch {

    final ModelUtils mu;

    private final long highestSeenOffset;

    private final TopicPartition tp;

    List<WorkContainer<String, String>> polledBatchWCs;

    List<ConsumerRecord<String, String>> polledBatch;

    EpochAndRecordsMap<String, String> polledRecordBatch;

    public PolledTestBatch(ModelUtils mu, TopicPartition tp, long fromOffset, long toOffset) {
        this.mu = mu;
        this.tp = tp;
        this.highestSeenOffset = toOffset;

        create(fromOffset, toOffset);
    }

    public PolledTestBatch(ModelUtils mu, TopicPartition tp, List<Long> polledOffsetsWithCompactedRemoved) {
        this.mu = mu;
        this.tp = tp;
        //noinspection OptionalGetWithoutIsPresent
        this.highestSeenOffset = JavaUtils.getLast(polledOffsetsWithCompactedRemoved).get();

        create(polledOffsetsWithCompactedRemoved);

    }

    void create(long fromOffset, long highestSeenOffset) {
        List<Long> offsets = LongStreamEx.range(fromOffset, highestSeenOffset + 1).boxed().toList();
        create(offsets);
    }

    void create(List<Long> offsets) {
        var offsetStream = StreamEx.of(offsets);
        this.polledBatchWCs = offsetStream
                .map(mu::createWorkFor)
                .toList();
        this.polledBatch = polledBatchWCs.stream()
                .map(WorkContainer::getCr)
                .collect(Collectors.toList());

        ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(UniMaps.of(tp, polledBatch));

        PartitionStateManager<String, String> mock = mock(PartitionStateManager.class);
        Mockito.when(mock.getEpochOfPartition(tp)).thenReturn(0L);
        this.polledRecordBatch = new EpochAndRecordsMap<>(consumerRecords, mock);
    }

}
