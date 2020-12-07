package io.confluent.parallelconsumer;

import io.confluent.csid.utils.KafkaTestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class BackPressureTests extends ParallelEoSStreamProcessorTestBase {

    /**
     * Tests that the backpressure system works correctly
     * - that when max queued messages are reached, more aren't queued up
     * - that more records aren't added for processing than are desired via settings.
     */
    @Test
    void backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing() {
        // mock messages downloaded for processing > MAX_TO_QUEUE
        // make sure work manager doesn't queue more than MAX_TO_QUEUE
        final int numRecords = 1_000;

        //
        int maxInFlight = 200;
        int maxQueue = 100;
        ParallelConsumerOptions<String, String> build = ParallelConsumerOptions.<String, String>builder()
                .build();
        WorkManager<String, String> wm = new WorkManager<>(build, consumerSpy);

        // add records
        {
            ConsumerRecords<String, String> crs = buildConsumerRecords(numRecords);
            wm.registerWork(crs);
        }

        //
        {
            List<WorkContainer<String, String>> work = wm.maybeGetWork();
            assertThat(work).hasSize(maxQueue);
            assertThat(wm.getNumberOfEntriesInPartitionQueues()).isEqualTo(maxInFlight);

            KafkaTestUtils.completeWork(wm, work, 50);
            KafkaTestUtils.completeWork(wm, work, 55);
        }

        // add more records
        {
            assertThat(wm.shouldThrottle()).isTrue();
            assertThat(wm.isSufficientlyLoaded()).isTrue();
            ConsumerRecords<String, String> crs = buildConsumerRecords(numRecords);
            wm.registerWork(crs);
            assertThat(wm.getNumberOfEntriesInPartitionQueues()).as("Hasn't increased").isEqualTo(maxInFlight);
        }

        // get more work
        {
            List<WorkContainer<String, String>> workContainers = wm.maybeGetWork();
            assertThat(workContainers).hasSize(2);
            assertThat(wm.getNumberOfEntriesInPartitionQueues()).isEqualTo(maxInFlight);
            assertThat(wm.shouldThrottle()).isTrue();
            assertThat(wm.isSufficientlyLoaded()).isTrue();
        }

    }

    private ConsumerRecords<String, String> buildConsumerRecords(final int numRecords) {
        List<ConsumerRecord<String, String>> consumerRecords = ktu.generateRecords(numRecords);
        Collections.sort(consumerRecords, Comparator.comparingLong(ConsumerRecord::offset));
        HashMap<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
        TopicPartition tp = new TopicPartition(INPUT_TOPIC, 0);
        recordsMap.put(tp, consumerRecords);
        ConsumerRecords<String, String> crs = new ConsumerRecords<>(recordsMap);
        return crs;
    }

}
