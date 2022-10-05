package io.confluent.parallelconsumer.state;

import io.confluent.csid.utils.Range;
import io.confluent.parallelconsumer.internal.EpochAndRecordsMap;
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

    public PolledTestBatch(ModelUtils mu, TopicPartition tp, long toOffset) {
        this.mu = mu;
        this.tp = tp;
        this.highestSeenOffset = toOffset;

        create(tp, toOffset);
    }

    void create(TopicPartition tp, long highestSeenOffset) {
        this.polledBatchWCs = Range.range(highestSeenOffset).toStream().boxed()
                .map(offset -> mu.createWorkFor(offset))
                .collect(Collectors.toList());
        this.polledBatch = polledBatchWCs.stream()
                .map(WorkContainer::getCr)
                .collect(Collectors.toList());

        ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(UniMaps.of(tp, polledBatch));

        PartitionStateManager<String, String> mock = mock(PartitionStateManager.class);
        Mockito.when(mock.getEpochOfPartition(tp)).thenReturn(0L);
        this.polledRecordBatch = new EpochAndRecordsMap<>(consumerRecords, mock);
    }

}
